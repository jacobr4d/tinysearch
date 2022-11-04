package com.jacobr4d.searcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jacobr4d.indexer.index.Index;
import com.jacobr4d.indexer.index.InvertedHit;
import com.sleepycat.persist.EntityCursor;

import spark.Service;

public class WebInterface {
	private static final Logger logger = LogManager.getLogger(WebInterface.class);
	
	Index index;
	static int MAX_URLS = 100;
	
	public WebInterface() {
		index = new Index("output/index");
		Service server = Service.ignite().port(45555).threadPool(10);
		server.get("/", (req, res) -> {
			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html>");
			html.append("<head>");
			html.append("</head>");
			html.append("<body>");
			html.append("<h1>Submit Query</h1>");
			html.append("<form method=\"POST\" action=\"/search\">");
			html.append("Query: <input type=\"text\" name=\"query\"/><br/>");
			html.append("<input type=\"submit\" value=\"Submit\">");
			html.append("</form>");
			html.append("<h1>Query Results</h1>");
	    	html.append("</body>");
	    	html.append("</html>");
	    	return html.toString();
		});	
		server.post("/search", (req, res) -> {
			String query = req.queryParams("query");
			String[] words = query.split("\\s+");
			String term = words[0];
			
			/* get matches, up to 100*/
			List<String> matches = new ArrayList<String>();
			EntityCursor<InvertedHit> hits = index.invertedHitsofWord(term);
			try {
				int count = 0;
				for (InvertedHit hit = hits.first(); hit != null && count++ < MAX_URLS; hit = hits.next()) {
					matches.add(hit.url);
				}
			} finally {
				hits.close();
			}
			
			System.out.println(matches);
			
			/* determine scores */
			List<String> scoreurls = new ArrayList<String>();
			for (String url : matches) {
				double tf = index.getTermFrequency(words[0], url);
				double idf = index.getInverseDocumentFrequency(term);
				double pr = index.getPageRank(url);
				double score = tf * idf * pr;
				scoreurls.add("" + score + " " + url);
			}
			
			/* display results on results page */
			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html>");
			html.append("<head>");
			html.append("</head>");
			html.append("<body>");
			html.append("Query: " + words[0]);
			html.append("<ul>");
			
			scoreurls.stream()
	        .sorted((a, b) -> {
	        	return Double.valueOf(b.split("\\s+")[0]).compareTo(Double.valueOf(a.split("\\s+")[0]));
	        })
	        .forEach((scoreurl) -> {
				html.append("<li>");
	    		html.append(scoreurl.split("\\s+")[0] + " <a href=\"" + scoreurl.split("\\s+")[1] + "\">" + scoreurl.split("\\s+")[1] + "</a>");
				html.append("</li>");
	        });
			
			html.append("</ul>");
			html.append("</body>");
			html.append("</html>");
        	res.type("text/html");
    		return html.toString();
    	});
	}
	
	public void close() {
		index.close();
	}
		
	
	public static void main(String args[]) throws IOException {
		WebInterface webInterface = new WebInterface();
		logger.info("Press [Enter] to shut down this node...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		webInterface.close();
		System.exit(0);
		
	}

}
