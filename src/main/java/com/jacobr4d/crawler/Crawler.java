package com.jacobr4d.crawler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import opennlp.tools.stemmer.PorterStemmer;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import com.jacobr4d.crawler.repository.Repository;
import com.jacobr4d.crawler.utils.HashUtils;
import com.jacobr4d.crawler.utils.HttpUtils;
import com.jacobr4d.crawler.utils.RobotsInfo;
import com.jacobr4d.crawler.utils.URLInfo;
import com.jacobr4d.indexer.index.Index;
import com.jacobr4d.indexer.index.InvertedHit;

import spark.Spark;

public class Crawler {
	private static final Logger logger = LogManager.getLogger(Crawler.class);
	
	/* FIELDS */
	public static int THREADS = 500;
	public static int MAX_FRONTIER_SIZE = 1000;
	public int maxDocSizeMB;
	Repository repo;
	
	/* STATE */
	public boolean quit = false;
	public AtomicInteger workersExited = new AtomicInteger(0);
	public AtomicInteger documentCount = new AtomicInteger(0);
	Queue<URLInfo> urlFrontier = new ConcurrentLinkedQueue<URLInfo>(); //keep capped
	Set<String> urlSet = new HashSet<String>();
	Set<String> hashSet = new HashSet<String>();
	public Map<String, RobotsInfo> robots = new HashMap<String, RobotsInfo>(); //sync
	
	/* INDEXER */
	public AtomicInteger hitCount = new AtomicInteger(0);
	public Set<String> stopwords = new HashSet<String>();
	public PorterStemmer stemmer = new PorterStemmer();
	Index index;
	FileWriter hitFileWriter;
	
	
    /* Constructor */
  	public Crawler(String maxSizeMB, String seedPath, String repoPath, String indexPath, String hitsPath) throws IOException, URISyntaxException {
		
		/* Indexer */
		stopwords = Files.lines(Paths.get("input/stopwords")).collect(Collectors.toSet());
		this.index = new Index(indexPath);
		if (!new File(hitsPath).getParentFile().exists() && !new File(hitsPath).getParentFile().mkdirs())
			throw new RuntimeException("unable to make dir " + new File(hitsPath).getParentFile());
		this.hitFileWriter = new FileWriter(hitsPath);
		
		Spark.port(45555);
		Spark.get("/", (req, res) -> {
			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html>");
			html.append("<head>");
			html.append("</head>");
			html.append("<body>");
			html.append("<h1>Crawler Status</h1>");
			html.append("<h2>FRONTIER SIZE (IN MEMORY): " + urlFrontier.size() + "</h2>");
			html.append("<h2>URL SET SIZE (IN MEMORY): " + urlSet.size() + "</h2>");
			html.append("<h2>HASH SET SIZE (IN MEMORY): " + hashSet.size() + "</h2>");
			html.append("<h2>ROBOTS SIZE (IN MEMORY) " + robots.size() + "</h2>");	
			html.append("<h2>DOC COUNT (ON DISK) " + documentCount.get() + "</h2>");	
			html.append("<h2>HIT COUNT (ON DISK) " + hitCount.get() + "</h2>");	
	    	html.append("</body>");
	    	html.append("</html>");
	    	return html.toString();
		});	
		
		/* CRAWLER */
		this.maxDocSizeMB = Integer.valueOf(maxSizeMB);
		this.repo = new Repository(repoPath);
		
		List<String> seedURLs = Files.lines(Paths.get("input/seed")).collect(Collectors.toList());
		for (String seedURL : seedURLs) {
			try {
				urlFrontier.add(new URLInfo(seedURL));
			} catch (RuntimeException e) {
				e.printStackTrace();
				continue;
			}
		}
		System.out.println(urlFrontier);
		
		for (int i = 0; i < THREADS; i++) {
			Worker worker = new Worker(this);
			worker.start();
		}
	}

	/* shut down gracefully and exit process */
	public void shutdown() throws IOException {	
		quit = true;
		while (workersExited.get() < THREADS)
			try {
				Thread.sleep(1000);
				logger.debug(workersExited.get() + " workers exited");
			} catch (Exception e) {
				logger.debug("master wait: " + e);
			}
		
		this.repo.close();
		this.index.close();
		this.hitFileWriter.close();
		
		/* DUMP STATE FOR DEBUGGING */
		logger.debug("URLFRONTIER");
		for (URLInfo url : urlFrontier)
			logger.debug(url);
		logger.debug("ROBOTS");
		for (String domain : robots.keySet()) {
			logger.debug(domain);
			logger.debug(robots.get(domain));
		}
		
	}

	public class Worker extends Thread {

		public Crawler crawler;

		public Worker(Crawler crawler) {
			this.crawler = crawler;
		}

		public void run(){
			while (!crawler.quit) {
				URLInfo url = crawler.urlFrontier.poll();
				if (url != null)
					process(url);
				else
					Thread.yield();
			}
			crawler.workersExited.incrementAndGet();
		}

		
		public void process(URLInfo url) {
				
			/* If we happen to have robots already, check if disallowed or delayed before first HEAD */
			synchronized (robots) {
				if (robots.containsKey(url.getHostName())) {
					RobotsInfo info = robots.get(url.getHostName());
					if (info.disallows(url)) {
						logger.debug(url + " disallowed by robots.txt before HEAD");
						return;
					}
					if (info.delays()) {
						logger.debug(url + " delayed by robots.txt before HEAD");
						urlFrontier.add(url);
						return;
					}	
				}
			}
			
			/* send HEAD, follow one redirect */
			HttpURLConnection head;
			try {
				head = HttpUtils.head(url);
				int code = head.getResponseCode();
				if (code == HttpURLConnection.HTTP_SEE_OTHER || code == HttpURLConnection.HTTP_MOVED_PERM || code == HttpURLConnection.HTTP_MOVED_TEMP) {
					String location = head.getHeaderField("Location");
					URLInfo oldURL = url.copy();
					if (location.startsWith("/")) {
						url = url.setFilePath(location);
					} else {
						url = new URLInfo(location);
					}
					head = HttpUtils.head(url);
					code = head.getResponseCode();
					if (code == HttpURLConnection.HTTP_OK) {
						logger.debug(oldURL + " redirected to " + url);
					} else {
						logger.debug(url + " responded to second head with " + code + ", ignoring...");
						return;
					}
				} else if (code != HttpURLConnection.HTTP_OK) {
					logger.debug(url + " responded to head with " + code + ", ignoring...");
					return;
				} 
			} catch (IOException e) {
				logger.debug(url + " head did not work");
				return;
			}

			/* see if HEAD checks pass */
			String modified = head.getHeaderField("Last-Modified");
			if (modified == null) 
				modified = head.getHeaderField("last-modified");
			String length = head.getHeaderField("Content-Length");
			if (length == null) 
				length = head.getHeaderField("content-length");
			String type = head.getHeaderField("Content-Type");
			if (type == null) 
				type = head.getHeaderField("content-type");
			
			head.disconnect();
			
			if (length == null) {
				logger.debug(url + " has no content-length, ignoring... ");
				return;
			}
			if (type == null) {
				logger.debug(url + " has no content-type, ignoring... ");
				return;
			}
			if (Integer.valueOf(length) > 1048576 * maxDocSizeMB) {
				logger.debug(url + " too big, ignoring...");
				return;
			}
			if (!type.startsWith("text/html")) {
				logger.debug(url + " not html / xml, ignoring...");
				return;
			}

			/* If we have the file, and it hasn't been changed, parse our version */
			com.jacobr4d.crawler.repository.Document doc = new com.jacobr4d.crawler.repository.Document();
			doc.url = url.toString();
			doc.contentType = type;
			
			com.jacobr4d.crawler.repository.Document stored = crawler.repo.getDocument(url);
			if (stored == null) {
				
				/* Get Robots, check if disallows or delays */
				RobotsInfo info = null;
				try {
					info = getOrCreateRobotsInfo(url);
				} catch (IOException e) {
					logger.debug(url + " robots.txt could not be retrieved, ignoring...");
					return;
				} catch (RuntimeException e) {
					logger.info("getorcreaterobots for " + url + " " + e);
					return;
				}
				if (info.disallows(url)) {
					logger.debug(url + " disallowed by robots.txt after HEAD, ignoring...");
					return;
				}
				if (info.delays()) {
					logger.debug(url + " delayed by robots.txt after HEAD");
					urlFrontier.add(url);
					return;
				}	
				
				/* GET */
				byte[] raw;
				try {
					HttpURLConnection get = HttpUtils.get(url);
					info.updateLastAccessed();
					if (get.getResponseCode() != HttpURLConnection.HTTP_OK) {
						logger.debug(url + " could not be accessed");
						return;
					} else {
						raw = IOUtils.toByteArray(get.getInputStream());
						doc.raw = raw;
						logger.info(url + " downloading...");
					}
					get.disconnect();
				} catch (IOException e) {
					logger.debug(url + " GET failed");
					return;
				}
				
			} else {
				logger.debug(url + " recovering from database...");
				doc = stored;
			}
			
			/* content seen check */
			String hash = HashUtils.md5(doc.raw);
			synchronized (hashSet) {
				if (hashSet.contains(hash)) {
					return;
				}
				hashSet.add(hash);
			}
			
			/* store document if we didn't recover it from database */
			if (stored == null)
				crawler.repo.putDocument(doc);
			logger.info(crawler.documentCount.incrementAndGet());
			
			/* parse document */
			org.jsoup.nodes.Document document = Jsoup.parse(new String(doc.raw, StandardCharsets.UTF_8), doc.url);
			
			/* Stemming steps:
			 * 1. remove common punctuation (,.)
			 * 1. filter is word [a-zA-Z]
			 * 3. regularize case (lowercase)
			 * 2. filter is not stop word
			 * 4. apply stemmer, ez with maven
			 * */
			/* print text (test) */
			for (String word : document.text().split("\\s+")) {
				word.replaceAll("[,.]", "");
				if (!word.matches("^[a-zA-Z]+$")) {
					continue;
				}
				word = word.toLowerCase();
				if (stopwords.contains(word)) {
					continue;
				}
				InvertedHit invertedHit = new InvertedHit();
				invertedHit.word = stemmer.stem(word);
				invertedHit.url = url.toString();
				index.putInvertedHit(invertedHit);
				try {
					hitFileWriter.write(word + "." + url + "\n");
				} catch (IOException e) {
					logger.error(e);
				}
				hitCount.incrementAndGet();
			}
			
			/* extract links */	
			if (urlFrontier.size() < MAX_FRONTIER_SIZE) {
				for (Element link : document.select("a[href]")) {
					String newURL = link.absUrl("href");
					try {
						URLInfo urlInfo = new URLInfo(newURL);
						/* check if seen url */
						if (!urlSet.contains(urlInfo.toString())) {
							urlSet.add(urlInfo.toString());
							urlFrontier.add(urlInfo);
						}
					} catch (RuntimeException e) {
						logger.debug(doc.url + " contained link we cant read " + link);
					}
				}
			}
		}
	} 


	public RobotsInfo getOrCreateRobotsInfo(URLInfo url) throws IOException {
		synchronized (robots) { //gotta make sure we dont get it twice
			if (robots.containsKey(url.getHostName())) {
				return robots.get(url.getHostName());
			} else {
				RobotsInfo info = createRobotsInfo(url);
				robots.put(url.getHostName(), info);
				return info;
			}
		}
	}
	
	public RobotsInfo createRobotsInfo(URLInfo url) throws IOException {
		HttpURLConnection con = HttpUtils.get(url.copy().setFilePath("/robots.txt"));
		if (con.getResponseCode() != HttpURLConnection.HTTP_OK) {
			logger.debug(url + " has no robots.txt, using default crawl delay...");
			return new RobotsInfo();
		} else {
			return new RobotsInfo(new String(con.getInputStream().readAllBytes(), StandardCharsets.UTF_8)); 
		}
	}
	
	public static void main(String args[]) throws IOException, URISyntaxException {

        if (args.length != 5) {
            logger.info("Usage: crawler [maxsizemb] [seed] [repo] [index] [hits]");
            System.exit(1);
        }

        Crawler crawler = new Crawler(args[0], args[1], args[2], args[3], args[4]);
        logger.debug("Press [Enter] to shut down...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		crawler.shutdown();
		System.exit(0);
	}

}
