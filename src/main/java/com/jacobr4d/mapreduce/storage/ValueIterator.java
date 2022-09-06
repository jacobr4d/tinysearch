package com.jacobr4d.mapreduce.storage;

import java.util.Iterator;

/* allow conversion from my KeyValuePair itertor to a String iterator */
public class ValueIterator implements Iterator<String> {

	Iterator<KeyValuePair> c;
	
	public ValueIterator(Iterator<KeyValuePair> c) {
		this.c = c;
	}
	
	@Override
	public boolean hasNext() {
		return c.hasNext();
	}

	@Override
	public String next() {
		return c.next().value;
	}

	
}
