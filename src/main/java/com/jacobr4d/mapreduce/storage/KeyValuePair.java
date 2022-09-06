package com.jacobr4d.mapreduce.storage;


import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class KeyValuePair {
	
	@PrimaryKey(sequence="doesntmatter")
	long myPrimaryKey; 
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	public String key;
	public String value;

	public KeyValuePair(String key, String value) {
		this.key = key;
		this.value = value;
	}
	
	/* Necessary */
	@SuppressWarnings("unused")
	private KeyValuePair() {}
}
