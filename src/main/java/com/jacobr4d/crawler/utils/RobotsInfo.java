package com.jacobr4d.crawler.utils;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jacobr4d.crawler.HttpAgent;

public class RobotsInfo {
	private static final Logger logger = LogManager.getLogger(RobotsInfo.class);

	/* If CDD not specified, "reasonable" is 1 second */
	public int delay = 0; 
	public Set<String> disallowedPaths = new HashSet<String>();
	public Instant lastSentGet = Instant.now();
	
	
	public RobotsInfo(String text) {
		String[] records = text.split("(\\r\\n){2,}|\\n{2,}");
		for (String record : records) { // split on 2 or more new lines
			for (String line : record.split("(\\r\\n)|\\n")) {
				line = stripComments(line);
				if (line.contains(":")) {
					if (getField(line).equals("user-agent")) {
						String userAgent = getValue(line);
						if (userAgent.equals("cis455crawler")) {
							parseRecord(record);
							return;
						} else if (userAgent.equals("*")) {
							parseRecord(record);
						}
					}
				}
			}
		}
	}

	public RobotsInfo() {
		// TODO Auto-generated constructor stub
	}

	public String toString() {
		return "delay: " + delay + "\n" + "disallowed: " + disallowedPaths.toString(); 
	}
	
	/* line # comment goes here */
	String stripComments(String line) {
		int i = line.indexOf("#");
		return i > -1 ? line.substring(0, i) : line;
	}

	/* <field>:<optionalspace><value><optionalspace> */
	String getField(String line) {
		int i = line.indexOf(":");
		if (i < 0)
			throw new RuntimeException("robots.txt line " + line + " has no :");
		String field = line.substring(0, i).trim().toLowerCase();
		if (field.isBlank())
			throw new RuntimeException("robots.txt line " + line + " has no field");
		return field;
	}

	/* <field>:<optionalspace><value><optionalspace> values can be empty for disallow */
	String getValue(String line) {
		int i = line.indexOf(":");
		if (i < 0)
			throw new RuntimeException("tried to get value but no :");
		return line.substring(i + 1).trim();
	}
	
	/* only parse disallow and crawl-delay fields */
	void parseRecord(String record) {
//		 logger.debug("----RECORD PARSED----");
//		 logger.debug(record);
//		 logger.debug("----RECORD----");
		for (String line : record.split("\\r\\n|\\r|\\n")) {
			line = stripComments(line);
			if (line.contains(":")) {
				if (getField(line).equals("disallow")) {
					String path = getValue(line);
					if (!path.isBlank()) {
						disallowedPaths.add(path);
					}
				} else if (getField(line).equals("crawl-delay")) {
					delay = Integer.parseInt(getValue(line));
				}
			}
		}
	}
	
	public boolean disallows(String url) {
		return disallows(new URLInfo(url));
	}
	
	public boolean disallows(URLInfo urlInfo) {
		String target = urlInfo.getFilePath();
		for (String disallowedPath : disallowedPaths) {
			if (target.startsWith(disallowedPath))
				return true;
		} 
		return false;
	}
	
	
	public boolean delays() {
		return Instant.now().isBefore(lastSentGet.plusSeconds(delay)) ;
	}
	
	public void updateLastAccessed() {
		lastSentGet = Instant.now();
	}

	public static void main(String[] args) throws IOException {
		URLInfo url = new URLInfo(args[0]);
		HttpAgent agent = new HttpAgent();
		RobotsInfo info = agent.getRobotsInfo(url);
		System.out.println(info);
	}

}
