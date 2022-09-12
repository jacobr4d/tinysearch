package com.jacobr4d.crawler;

import java.util.LinkedList;
import java.util.Queue;

import com.jacobr4d.crawler.utils.URLInfo;

public class QueueFrontier implements Frontier {
	
	Queue<URLInfo> queue = new LinkedList<URLInfo>();

	@Override
	public URLInfo poll() {
		return queue.poll();
	}

	@Override
	public void add(URLInfo url) {
		queue.add(url);
	}

	@Override
	public int size() {
		return queue.size();
	}


}
