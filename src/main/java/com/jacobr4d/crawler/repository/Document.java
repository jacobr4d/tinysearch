package com.jacobr4d.crawler.repository;

import java.time.Instant;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class Document {
	
	@PrimaryKey(sequence="docId")
	public long docId;
    @SecondaryKey(relate=Relationship.ONE_TO_ONE)
	public String url;
	public long crawledTime = Instant.now().getEpochSecond();
	public String contentType;
	public byte[] raw;
    
    public Document() {} //for bindings
    

}
