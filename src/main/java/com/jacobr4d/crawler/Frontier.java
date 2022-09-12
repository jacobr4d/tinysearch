package com.jacobr4d.crawler;

import com.jacobr4d.crawler.utils.URLInfo;

public interface Frontier {

	public URLInfo poll();
	
	public void add(URLInfo url);
	
	public int size();
	
}
