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
	public EntityStore store;
	
	/* INVERTED HITS */
	public PrimaryIndex<Long, InvertedHit> invertedHitIndex;
	public SecondaryIndex<String, Long, InvertedHit> invertedHitsByWord;
	
	/* TFS */
	public PrimaryIndex<Long, TermFrequency> termFrequencyIndex;
	public SecondaryIndex<String, Long, TermFrequency> termFrequencyByWordUrl;
	
	/* IDFS */
	public PrimaryIndex<Long, InverseDocumentFrequency> inverseDocumentFrequencyIndex;
	public SecondaryIndex<String, Long, InverseDocumentFrequency> inverseDocumentFrequencyByWord;
	
	/* PAGERANKS */
	public PrimaryIndex<Long, PageRank> pageRankIndex;
	public SecondaryIndex<String, Long, PageRank> pageRankByUrl;
	
	public Index(String envPath) {
		this.envPath = envPath;
		
		/* make storage dir if doesn't exist */
		if (!new File(envPath).exists() && !new File(envPath).mkdirs())
			throw new RuntimeException("bdb unable to make dir " + envPath);
		
		env = new Environment((new File(envPath)), new EnvironmentConfig().setAllowCreate(true));
		store = new EntityStore(env, "store", new StoreConfig().setAllowCreate(true));
		
		invertedHitIndex = store.getPrimaryIndex(Long.class, InvertedHit.class);
		invertedHitsByWord = store.getSecondaryIndex(invertedHitIndex, String.class, "word");
		
		termFrequencyIndex = store.getPrimaryIndex(Long.class, TermFrequency.class);
		termFrequencyByWordUrl = store.getSecondaryIndex(termFrequencyIndex, String.class, "wordUrl");
		
		inverseDocumentFrequencyIndex = store.getPrimaryIndex(Long.class, InverseDocumentFrequency.class);
		inverseDocumentFrequencyByWord = store.getSecondaryIndex(inverseDocumentFrequencyIndex, String.class, "word");
		
		pageRankIndex = store.getPrimaryIndex(Long.class, PageRank.class);
		pageRankByUrl = store.getSecondaryIndex(pageRankIndex, String.class, "url");
	}

	public void close() {
		store.close();
	    env.close();
	}
	
	
	/* INVERTED INDEX */
	
	public void putInvertedHit(InvertedHit invertedHit) {
		invertedHitIndex.putNoReturn(invertedHit);
	} 
	
	public EntityCursor<InvertedHit> invertedHitsofWord(String word) {
		return invertedHitsByWord.subIndex(word).entities();
	}
	
	public EntityCursor<String> words() {
		return invertedHitsByWord.keys();
	}
	
	
	/* TFS */
	
	public void putTermFrequency(TermFrequency tf) {
		termFrequencyIndex.putNoReturn(tf);
	}
	
	public double getTermFrequency(String word, String url) {
		return Double.valueOf(termFrequencyByWordUrl.get(word + " " + url).frequency);
	}
	
	
	/* IDFS */
	
	public void putInverseDocumentFrequency(InverseDocumentFrequency idf) {
		inverseDocumentFrequencyIndex.putNoReturn(idf);
	}
	
	public double getInverseDocumentFrequency(String word) {
		return Double.valueOf(inverseDocumentFrequencyByWord.get(word).inverseDocumentFrequency);
	}
	
	
	/* PAGERANKS */
	
	public void putPageRank(PageRank pageRank) {
		pageRankIndex.putNoReturn(pageRank);
	}
	
	public double getPageRank(String url) {
		return Double.valueOf(pageRankByUrl.get(url).pageRank);
	}
	
}





