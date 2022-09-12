package com.jacobr4d.crawler;

import java.util.HashSet;
import java.util.Set;

import com.jacobr4d.crawler.utils.URLInfo;

public class URLSet {
	
	Crawler crawler;
	Set<String> urlSet = new HashSet<String>();
	
	public URLSet(Crawler crawler) {
		this.crawler = crawler;
	}

	public void submitURL(URLInfo url) {
		if (!urlSet.contains(url.toString())) {
			urlSet.add(url.toString());
			crawler.addURL(url);
		}
	}
	
	public int size() {
		return urlSet.size();
	}

}
