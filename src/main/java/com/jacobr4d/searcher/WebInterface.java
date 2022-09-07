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
			
			/* get relevant urls */
			List<String> urlList = new ArrayList<String>();
			EntityCursor<InvertedHit> hits = index.invertedHitsofWord(words[0]);
			try {
				int count = 0;
				for (InvertedHit hit = hits.first(); hit != null && count++ < MAX_URLS; hit = hits.next()) {
					urlList.add(hit.url);
				}
			} finally {
				hits.close();
			}
			
			System.out.println(urlList);
			
			/* determine scores */
			List<String> outputs = new ArrayList<String>();
			for (String url : urlList) {
				double tf = index.getTermFrequency(words[0], url);
				double idf = index.getInverseDocumentFrequency(words[0]);
				double score = tf * idf;
				outputs.add("" + score + " " + url);
			}
			
			
			/* build response */
			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html>");
			html.append("<head>");
			html.append("</head>");
			html.append("<body>");
			html.append("Query: " + words[0]);
			html.append("<ul>");
			
			outputs.stream()
	        .sorted((a, b) -> {
	        	String[] wordsA = a.split("\\s+");
	        	String[] wordsB = b.split("\\s+");
	        	Double scoreA = Double.valueOf(wordsA[0]);
	        	Double scoreB = Double.valueOf(wordsB[0]);
	        	return scoreB.compareTo(scoreA);
	        })
	        .forEach((output) -> {
				html.append("<li>");
				html.append(output);
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
