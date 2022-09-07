package com.jacobr4d.indexer.index;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class InverseDocumentFrequency {

	@PrimaryKey(sequence="invertedHitId")
     long id;

     @SecondaryKey(relate=Relationship.ONE_TO_ONE)
     public String word;
     public String inverseDocumentFrequency;

     public InverseDocumentFrequency() {}
}
