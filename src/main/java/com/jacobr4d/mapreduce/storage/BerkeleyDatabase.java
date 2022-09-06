package com.jacobr4d.mapreduce.storage;


import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;


public class BerkeleyDatabase {
	
	/* static after init */
	String envPath;
	
	/* dynamic */
	Environment env;
	EntityStore store;
	PrimaryIndex<Long, KeyValuePair> index; 				// (key, value) by long uid (not useful but required by bdb)
	SecondaryIndex<String, Long, KeyValuePair> second;		// (key, value) by key
	
	/* make dir for db and make open db itself */
	public BerkeleyDatabase(String path) {
		
		/* make dir for database at path */
		if (!new File(path).mkdir())
        	throw new RuntimeException("bdb dir could not be created");
		
		this.envPath = path;

		open();
	}

	/* open environment */
	public void open() {
		env = new Environment((new File(envPath)), new EnvironmentConfig().setAllowCreate(true));
		store = new EntityStore(env, "store", new StoreConfig().setAllowCreate(true));
		index = store.getPrimaryIndex(Long.class, KeyValuePair.class);
		second = store.getSecondaryIndex(index, String.class, "key");
	}
	
	/* insert (key, value) */
	public void put(KeyValuePair kvpair) {
		index.putNoReturn(kvpair);
	}
	
	/* cursor over (key, *) */
	public EntityCursor<KeyValuePair> valueCursor(String key) {
		return second.subIndex(key).entities();
	}
	
	/* cursor over keys (not unique, which is weird) 
	 * best to use this then valueCursor inside loop */
	public EntityCursor<String> keyCursor() {
		return second.keys();
	}
	
	/* destroy db itself */
	public void destroy() {

	    /* don't care about closing env or store, just delete whole dir */
	    File dir = new File(envPath);
	    for (File file: dir.listFiles()) 
	    	if (!file.delete())
	    		throw new RuntimeException("berkeley db: couldn't remove db file");
	    if (!dir.delete())
    		throw new RuntimeException("berkeley db: couldn't remove db dir");
	    
	}
}