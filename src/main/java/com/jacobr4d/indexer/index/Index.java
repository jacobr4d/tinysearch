package com.jacobr4d.indexer.index;

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;


public class Index {
	
	public String envPath;
	public Environment env;
	EntityStore store;
	PrimaryIndex<Long, InvertedHit> invertedHitIndex;
	SecondaryIndex<String, Long, InvertedHit> invertedHitsByWord;
	
	public Index(String envPath) {
		this.envPath = envPath;
		
		/* make storage dir if doesn't exist */
		if (!new File(envPath).exists() && !new File(envPath).mkdirs())
			throw new RuntimeException("bdb unable to make dir " + envPath);
		
		env = new Environment((new File(envPath)), new EnvironmentConfig().setAllowCreate(true));
		store = new EntityStore(env, "store", new StoreConfig().setAllowCreate(true));
		
		invertedHitIndex = store.getPrimaryIndex(Long.class, InvertedHit.class);
		invertedHitsByWord = store.getSecondaryIndex(invertedHitIndex, String.class, "word");
	}

	public void close() {
		store.close();
	    env.close();
	}
	
	public void putInvertedHit(InvertedHit invertedHit) {
		invertedHitIndex.putNoReturn(invertedHit);
	}
	
	/* get urls associated with a word */
	public EntityCursor<InvertedHit> invertedHitsofWord(String word) {
		return invertedHitsByWord.subIndex(word).entities();
	}
	
	public EntityCursor<String> words() {
		return invertedHitsByWord.keys();
	}
}
