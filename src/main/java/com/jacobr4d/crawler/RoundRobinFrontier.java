package com.jacobr4d.crawler;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.jacobr4d.crawler.utils.URLInfo;

public class RoundRobinFrontier implements Frontier {
	
	public static int NUM_QUEUES = 12;
	public static AtomicInteger cur = new AtomicInteger(0); 

	/* several queues, domain is hashed */
	ArrayList<ConcurrentLinkedQueue<URLInfo>> queues = new ArrayList<ConcurrentLinkedQueue<URLInfo>>();
	
	public RoundRobinFrontier() {
		for (int i = 0; i < NUM_QUEUES; i++)
			queues.add(new ConcurrentLinkedQueue<URLInfo>());
	}
	
	public synchronized URLInfo poll() {
		if (cur.intValue() == NUM_QUEUES - 1)
			cur.set(0);
		else 
			cur.incrementAndGet();		
		return queues.get(cur.get()).poll();
	}
	
	public void add(URLInfo url) {
		queues.get(Math.abs(url.hashCode()) % NUM_QUEUES).add(url);
	}
	
	public int size() {
		int size = 0;
		for (ConcurrentLinkedQueue<URLInfo> queue : queues) {
			size += queue.size();
		}
		return size;
	}

}
