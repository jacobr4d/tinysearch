package com.jacobr4d.crawler.repository;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class Channel {
	
	@PrimaryKey(sequence="channelId")
	public long channelId;
    @SecondaryKey(relate=Relationship.ONE_TO_ONE)
	public String channelName;
	public String pattern;
	public String creator;
	
	public Channel() {} // used for bindings
//	
//	public String toString() {
//		return "[" + channelId + ", " 
//				+ name + ", "
//				+ pattern + ", "
//				+ userId + ", "
//				+ docIds.toString() + "]";
//	}
}