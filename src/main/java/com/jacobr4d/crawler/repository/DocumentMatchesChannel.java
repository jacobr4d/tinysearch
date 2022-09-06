package com.jacobr4d.crawler.repository;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class DocumentMatchesChannel {

	@PrimaryKey(sequence="matchId")
     long id;

     @SecondaryKey(relate=Relationship.MANY_TO_ONE)
     public String documentURL;

     @SecondaryKey(relate=Relationship.MANY_TO_ONE)
     public String channelName;

     public DocumentMatchesChannel() {}
}
