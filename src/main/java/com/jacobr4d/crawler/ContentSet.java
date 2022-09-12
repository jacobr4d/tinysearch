package com.jacobr4d.crawler;

import java.util.HashSet;
import java.util.Set;

public class ContentSet {
	
	Set<String> hashes = new HashSet<String>();

	/* returns true if seen before */
	public synchronized boolean isDuplicateContent(String hash) {
		if (!hashes.contains(hash)) {
			hashes.add(hash);
			return false;
		} else {
			return true;
		}
	}
		
	public int size() {
		return hashes.size();
	}

}
