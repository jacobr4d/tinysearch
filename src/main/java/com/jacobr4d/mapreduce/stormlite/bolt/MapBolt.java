package com.jacobr4d.mapreduce.stormlite.bolt;

import java.util.Map;
import java.util.UUID;

import com.jacobr4d.mapreduce.Job;
import com.jacobr4d.mapreduce.stormlite.OutputFieldsDeclarer;
import com.jacobr4d.mapreduce.stormlite.TopologyContext;
import com.jacobr4d.mapreduce.stormlite.TopologyContext.STATE;
import com.jacobr4d.mapreduce.stormlite.distributed.ConsensusTracker;
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.tuple.Fields;
import com.jacobr4d.mapreduce.stormlite.tuple.Tuple;

/**
 * A simple adapter that takes a MapReduce "Job" and calls the "map"
 * on a per-tuple basis.
 * 
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

public class MapBolt implements IRichBolt {

	/* static */
	Fields schema = new Fields("key", "value");
    String executorId = UUID.randomUUID().toString();

    /* set at prepare */
    OutputCollector collector;
    TopologyContext context;
	Job mapJob;
	ConsensusTracker votesForEos;

	/* dynamic */
	boolean sentEos = false;
	
    
    @SuppressWarnings("deprecation")
	@Override
    public void prepare(Map<String,String> config, TopologyContext context, OutputCollector collector) {
    	
    	/* make sure we have what we need from config */
    	if (!config.containsKey("mapClass"))
        	throw new RuntimeException("Mapper bolt doesn't know mapper class ");

        if (!config.containsKey("spoutExecutors"))
        	throw new RuntimeException("Mapper bolt doesn't know how many input spout executors");
        
        if (!config.containsKey("numWorkers"))
        	throw new RuntimeException("Mapper bolt doesn't know how many workers");
    	
        this.collector = collector;
        this.context = context;

        try {
			mapJob = (Job) Class.forName(config.get("mapClass")).newInstance();
	        votesForEos = new ConsensusTracker(Integer.valueOf(config.get("spoutExecutors")) * 
	        		Integer.valueOf(config.get("numWorkers")));
        } catch (Exception e ) {
			throw new RuntimeException("mapbolt prepare: " + e);
        }
    }

    @Override
    public synchronized boolean execute(Tuple input) {
    	if (!input.isEndOfStream()) {
//          System.out.println("mapper " + getExecutorId() + " received " + input.toString() + " from " + input.getSourceExecutor());

    		/* this executor tells its worker that the worker should consider itself mapping, if not already */
            context.declareState(STATE.MAPPING);
           
	        context.incrementKeysRead();	        
	        mapJob.map(input.getStringByField("key"), input.getStringByField("value"), collector, executorId);
	        context.incrementKeysWritten();

    	} else {
//	        System.out.println("mapper " + getExecutorId() + " received EOS from " + input.getSourceExecutor());
	        
    		/* this executor totally done! */
    		if (votesForEos.voteForEos(input.getSourceExecutor())) {
                collector.emitEndOfStream(getExecutorId());
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
