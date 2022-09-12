package com.jacobr4d.crawler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jacobr4d.crawler.utils.HttpUtils;
import com.jacobr4d.crawler.utils.RobotsInfo;
import com.jacobr4d.crawler.utils.URLInfo;

public class RobotsMap {
	private static final Logger logger = LogManager.getLogger(RobotsMap.class);

	public Map<String, RobotsInfo> robots = new HashMap<String, RobotsInfo>(); //sync
	
	public int size() {
		return robots.size();
	}
	
	public RobotsInfo getDontCreate(URLInfo url) {
		return robots.get(url.getHostName());
	}

	public RobotsInfo getOrCreate(URLInfo url, HttpAgent httpAgent) throws IOException {
		if (robots.containsKey(url.getHostName())) {
			return robots.get(url.getHostName());
		} else {
			logger.debug("parsing robots, " + url.getHostName());
			RobotsInfo info = httpAgent.getRobotsInfo(url);
			robots.put(url.getHostName(), info);
			return info;
		}
	}
	
	

}
