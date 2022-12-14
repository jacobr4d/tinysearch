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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import com.jacobr4d.crawler.utils.HashUtils;
import com.jacobr4d.crawler.utils.RobotsInfo;
import com.jacobr4d.crawler.utils.URLInfo;

import opennlp.tools.stemmer.PorterStemmer;
import spark.Spark;

public class Crawler {
	private static final Logger logger = LogManager.getLogger(Crawler.class);
	
	/* FIELDS */
	public static int THREADS = 100;
	public int maxDocSizeMB;
	
	/* STATE */
	public boolean quit = false;
	public AtomicInteger workersExited = new AtomicInteger(0);
	public AtomicInteger documentCount = new AtomicInteger(0);
	List<Worker> workers = new ArrayList<Worker>();
	URLSet urlSet = new URLSet(this);
	ContentSet contentSet = new ContentSet();
	
	/* INDEXER */
	public AtomicInteger hitCount = new AtomicInteger(0);
	public Set<String> stopwords = new HashSet<String>();
	public PorterStemmer stemmer = new PorterStemmer();
	FileWriter hitFileWriter;
	FileWriter linkFileWriter;
	
    /* Constructor */
  	public Crawler(String maxSizeMB, String seedPath, String hitsPath, String linksPath) throws IOException, URISyntaxException {
		
		/* Indexer */
		this.stopwords = Files.lines(Paths.get("input/stopwords")).collect(Collectors.toSet());
		
		if (!new File(hitsPath).getParentFile().exists() && !new File(hitsPath).getParentFile().mkdirs())
			throw new RuntimeException("unable to make dir " + new File(hitsPath).getParentFile());
		if (!new File(linksPath).getParentFile().exists() && !new File(linksPath).getParentFile().mkdirs())
			throw new RuntimeException("unable to make dir " + new File(linksPath).getParentFile());
		
		this.hitFileWriter = new FileWriter(hitsPath);
		this.linkFileWriter = new FileWriter(linksPath);
		
		Spark.port(45555);
		Spark.get("/", (req, res) -> {
			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html>");
			html.append("<head>");
			html.append("</head>");
			html.append("<body>");
			html.append("<h1>Crawler Status</h1>");
			int frontierSize = 0;
			for (Worker worker : workers)
				frontierSize += worker.urlFrontier.size();
			html.append("<h2>FRONTIER SIZE (IN MEMORY): " + frontierSize + "</h2>");
			html.append("<h2>URL SET SIZE (IN MEMORY): " + urlSet.size() + "</h2>");
			html.append("<h2>CONTENT SET SIZE (IN MEMORY): " + contentSet.size() + "</h2>");
			int robotsSize = 0;
			for (Worker worker : workers)
				robotsSize += worker.robots.size();
			html.append("<h2>ROBOTS SIZE (IN MEMORY) " + robotsSize + "</h2>");	
			html.append("<h2>DOC COUNT (ON DISK) " + documentCount.get() + "</h2>");	
			html.append("<h2>HIT COUNT (ON DISK) " + hitCount.get() + "</h2>");	
	    	html.append("</body>");
	    	html.append("</html>");
	    	return html.toString();
		});	
		
		/* CRAWLER */
		this.maxDocSizeMB = Integer.valueOf(maxSizeMB);
		
		for (int i = 0; i < THREADS; i++) {
			workers.add(new Worker(this));
		}
		
		for (String seedURL : Files.lines(Paths.get("input/seed")).collect(Collectors.toList())) {
			try {
				addURL(new URLInfo(seedURL));
			} catch (RuntimeException e) {
				e.printStackTrace();
				continue;
			}
		}		

		for (int i = 0; i < THREADS; i++)
			workers.get(i).start();

	}
  	
  	/* add url based on its host to some frontier */
  	public void addURL(URLInfo url) {
  		workers.get(Math.abs(url.hashCode()) % THREADS).urlFrontier.add(url);
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
		
//		this.repo.close();
//		this.index.close();
		this.hitFileWriter.close();		
		this.linkFileWriter.close();
	}

	public class Worker extends Thread {

		public Crawler crawler;
		HttpAgent httpAgent = new HttpAgent();
		Frontier urlFrontier = new QueueFrontier();
		public Map<String, RobotsInfo> robots = new HashMap<String, RobotsInfo>();


		public Worker(Crawler crawler) {
			this.crawler = crawler;
		}

		public void run(){
			while (!crawler.quit) {
				URLInfo url = urlFrontier.poll();
				if (url != null)
					process(url);
				else
					Thread.yield();
			}
			crawler.workersExited.incrementAndGet();
		}

		
		public void process(URLInfo url) {
				
			/* If we happen to have robots already, check if disallowed or delayed before first HEAD */
			RobotsInfo info = robots.get(url.getHostName());

			if (info != null) {
				if (info.disallows(url)) {
					logger.debug("disallowed " + url);
					return;
				}
				if (info.delays()) {
					logger.debug("delayed " + url);
					urlFrontier.add(url);
					Thread.yield();
					return;
				}	
			}
			
			/* SEND HEAD, do some checks */
			HttpURLConnection head;
			try {
				head = httpAgent.head(url);
				int code = head.getResponseCode();
				if (code == HttpURLConnection.HTTP_SEE_OTHER || code == HttpURLConnection.HTTP_MOVED_PERM || code == HttpURLConnection.HTTP_MOVED_TEMP) {
					String location = head.getHeaderField("Location");
//					URLInfo oldURL = url.copy();
					if (location.startsWith("/")) {
						url = url.setFilePath(location);
					} else {
						url = new URLInfo(location);
					}
					head = httpAgent.head(url);
					code = head.getResponseCode();
					if (code == HttpURLConnection.HTTP_OK) {
						logger.debug("redirected ");
					} else {
						logger.debug("inaccessible ");
						return;
					}
				} else if (code != HttpURLConnection.HTTP_OK) {
					logger.debug("head failed, non 200 response");
					return;
				} 
			} catch (IOException e) {
				logger.debug("head failed, exception " + e);
				return;
			}

			String modified = head.getHeaderField("Last-Modified");
			if (modified == null) 
				modified = head.getHeaderField("last-modified");
			String length = head.getHeaderField("Content-Length");
			if (length == null) 
				length = head.getHeaderField("content-length");
			String type = head.getHeaderField("Content-Type");
			if (type == null) 
				type = head.getHeaderField("content-type");
						
			if (length == null) {
				logger.debug("no length ");
				return;
			}
			if (type == null) {
				logger.debug("no type ");
				return;
			}
			if (Integer.valueOf(length) > 1048576 * maxDocSizeMB) {
				logger.debug("invalid size ");
				return;
			}
			if (!type.startsWith("text/html")) {
				logger.debug("invalid type");
				return;
			}

			/* GET ROBOTS, do some checks */
			try {
				if (robots.containsKey(url.getHostName())) {
					info = robots.get(url.getHostName());
				} else {
					logger.debug("parsing robots, " + url.getHostName());
					info = httpAgent.getRobotsInfo(url);
					robots.put(url.getHostName(), info);
				}
			} catch (IOException e) {
				logger.debug("inaccessible robots");
				return;
			} catch (RuntimeException e) {
				logger.debug("errant robots");
				return;
			}
			if (info.disallows(url)) {
				logger.debug("disallowed, " + url);
				return;
			}
			if (info.delays()) {
				logger.debug("delayed, " + url);
				urlFrontier.add(url);
				Thread.yield();
				return;
			}	
			
			/* GET */
			byte[] raw;
			try {
				HttpURLConnection get = httpAgent.get(url);
				info.updateLastAccessed();
				if (get.getResponseCode() != HttpURLConnection.HTTP_OK) {
					logger.debug("inaccessible");
					return;
				} else {
					raw = IOUtils.toByteArray(get.getInputStream());
					logger.debug("downloading");
				}
			} catch (IOException e) {
				logger.debug("get failed");
				return;
			}
				
			
			/* content seen check */
			String hash = HashUtils.md5(raw);
			if (contentSet.isDuplicateContent(hash)) {
				logger.debug("duplicate contents");
				return;
			}
				
			logger.info(crawler.documentCount.incrementAndGet());
			
			/* parse document */
			org.jsoup.nodes.Document document = Jsoup.parse(new String(raw, StandardCharsets.UTF_8), url.toString());
			
			/* Stemming steps:
			 * 1. remove common punctuation (,.)
			 * 1. filter is word [a-zA-Z]
			 * 3. regularize case (lowercase)
			 * 2. filter is not stop word
			 * 4. apply stemmer, ez with maven (not stemming cause error :/)
			 * */
			/* print text (test) */
			for (String word : document.text().split("\\s+")) {
				word = word.replaceAll("[,.]", "").toLowerCase();
				if (!word.matches("^[a-z]+$"))
					continue;
				if (stopwords.contains(word))
					continue;
				try {
					hitFileWriter.write(word + " " + url + "\n");
				} catch (IOException e) {
					logger.error(e);
				}
				hitCount.incrementAndGet();
			}
			
			/* extract links */	
			for (Element link : document.select("a[href]")) {
				String newURLString = link.absUrl("href");
				try {
					URLInfo newURL = new URLInfo(newURLString);
					
					linkFileWriter.write(url.toString() + " " + newURL.toString() + "\n");
					
					/* check if seen url */
					urlSet.submitURL(newURL);
				} catch (RuntimeException e) {
					logger.debug("link unparsible");
				} catch (IOException e) {
					logger.error(e);
				}
			}
		}
	} 
	
	public static void main(String args[]) throws IOException, URISyntaxException {

        if (args.length != 4) {
            logger.info("Usage: crawler [maxsizemb] [seedPath] [hitsPath] [linksPath]");
            System.exit(1);
        }

        Crawler crawler = new Crawler(args[0], args[1], args[2], args[3]);
        logger.debug("Press [Enter] to shut down...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		crawler.shutdown();
		System.exit(0);
	}

}
