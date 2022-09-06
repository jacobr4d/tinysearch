package com.jacobr4d.mapreduce.stormlite.bolt;

import java.util.Map;
import java.util.UUID;

import com.jacobr4d.mapreduce.Job;
import com.jacobr4d.mapreduce.storage.BerkeleyDatabase;
import com.jacobr4d.mapreduce.storage.KeyValuePair;
import com.jacobr4d.mapreduce.storage.ValueIterator;
import com.jacobr4d.mapreduce.stormlite.OutputFieldsDeclarer;
import com.jacobr4d.mapreduce.stormlite.TopologyContext;
import com.jacobr4d.mapreduce.stormlite.TopologyContext.STATE;
import com.jacobr4d.mapreduce.stormlite.distributed.ConsensusTracker;
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.tuple.Fields;
import com.jacobr4d.mapreduce.stormlite.tuple.Tuple;
import com.sleepycat.persist.EntityCursor;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "reduce"
 * on a per-tuple basis
 * 
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ReduceBolt implements IRichBolt {
	
	/* static */
	Fields schema = new Fields("key", "value");
    String executorId = UUID.randomUUID().toString();
	
	/* set at prepare */
    OutputCollector collector;
    TopologyContext context;
	Job reduceJob;
    BerkeleyDatabase db;
	ConsensusTracker votesForEos;
	
	/* dynamic */
    boolean sentEos = false;


    /**
     * Initialization, just saves the output stream destination
     */
    @SuppressWarnings("deprecation")
	@Override
    public void prepare(Map<String,String> config, TopologyContext context, OutputCollector collector) {
    	
    	if (!config.containsKey("reduceClass"))
        	throw new RuntimeException("Reduce bolt doesn't know reducer class");
        
        if (!config.containsKey("mapExecutors"))
        	throw new RuntimeException("Reducer bolt doesn't know how many map bolt executors");
        
        if (!config.containsKey("numWorkers"))
        	throw new RuntimeException("Reducer bolt doesn't know how many workers");
        
        if (!config.containsKey("storageDir"))
        	throw new RuntimeException("Reducer bolt doesn't know storageDir");
        
        this.collector = collector;
        this.context = context;
        
        try {
			reduceJob = (Job)Class.forName(config.get("reduceClass")).newInstance();
	        db = new BerkeleyDatabase(config.get("storageDir") + "/" + getExecutorId());
	        votesForEos = new ConsensusTracker(Integer.valueOf(config.get("mapExecutors")) * 
	        		Integer.valueOf(config.get("numWorkers")));
        } catch (Exception e) {
        	throw new RuntimeException("reducerbolt prepare: " + e);
        }
    }
    
    

    /**
     * Process a tuple received from the stream, buffering by key
     * until we hit end of stream
     */
    @Override
    public synchronized boolean execute(Tuple input) {
    	if (!input.isEndOfStream()) {

    		/* store tuple in database until reduce phase */
	        db.put(new KeyValuePair(input.getStringByField("key"), input.getStringByField("value")));
	        
    	} else if (!sentEos) {
//			System.out.println("reducer " + getExecutorId() + " received EOS from " + input.getSourceExecutor());
    		
    		/* start and complete reduce phase */
       		if (votesForEos.voteForEos(input.getSourceExecutor())) {
    			
       			/* officially reducing, msg from this executor thread to the worker process */
                context.declareState(STATE.REDUCING);
    			
                EntityCursor<String> keyCursor = db.keyCursor();
        		try {
        			for (String key = keyCursor.first(); key != null; key = keyCursor.nextNoDup()) {
        				context.incrementKeysRead();
        				EntityCursor<KeyValuePair> valueCursor = db.valueCursor(key);
        				try {
	            			reduceJob.reduce(key, new ValueIterator(valueCursor.iterator()), collector, getExecutorId());
        				} finally {
        					valueCursor.close();
        				}
        				context.incrementKeysWritten();
        			}
        		} finally {
        			keyCursor.close();
        		}
    			
        		/* this executor done! */
                collector.emitEndOfStream(getExecutorId());
    			sentEos = true;
    		}
    	}
    	/* doesn't matter what we return */
    	return true;
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	db.destroy();
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(StreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}

}
