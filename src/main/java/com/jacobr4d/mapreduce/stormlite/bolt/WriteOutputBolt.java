package com.jacobr4d.mapreduce.stormlite.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.jacobr4d.mapreduce.stormlite.OutputFieldsDeclarer;
import com.jacobr4d.mapreduce.stormlite.TopologyContext;
import com.jacobr4d.mapreduce.stormlite.TopologyContext.STATE;
import com.jacobr4d.mapreduce.stormlite.distributed.ConsensusTracker;
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.tuple.Fields;
import com.jacobr4d.mapreduce.stormlite.tuple.Tuple;

public class WriteOutputBolt implements IRichBolt {
	
	/* static */
	Fields myFields = new Fields();
    String executorId = UUID.randomUUID().toString();

    /** set at prepare */
	TopologyContext context;
    BufferedWriter writer;
	ConsensusTracker votesForEos;
	
	
	@Override
	public void prepare(Map<String, String> config, TopologyContext context, OutputCollector collector) {
		
        if (!config.containsKey("reduceExecutors"))
        	throw new RuntimeException("writeOutputBolt doesn't know reduceExecutors");
        
        if (!config.containsKey("numWorkers"))
        	throw new RuntimeException("writeOutputBolt doesn't know numWorkers");
        
        if (!config.containsKey("storageDir"))
        	throw new RuntimeException("writeOutputBolt doesn't know storage dir");
        
        if (!config.containsKey("outputFile"))
        	throw new RuntimeException("writeOutputBolt doesn't know outputFile");
        
		this.context = context;
		
		try {
 			writer = new BufferedWriter(new FileWriter(
 					config.get("storageDir") + "/" + config.get("outputFile")));
	        votesForEos = new ConsensusTracker(Integer.valueOf(config.get("reduceExecutors")) * 
	        		Integer.valueOf(config.get("numWorkers")));
		} catch (IOException e) {
			throw new RuntimeException("writeOutputBolt prepare: " + e);
		}
	}
	
	@Override
	public void cleanup() {
		try {
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException("writeOutputBolt cleanup: " + e);
		}
	}

	@Override
	public boolean execute(Tuple input) {
		if (!input.isEndOfStream()) {
			try {
				String token = input.getStringByField("key") + " " + input.getStringByField("value");
				writer.write(token + "\n");
				context.logOutput(token);
				writer.flush();
			} catch (IOException e) {
				throw new RuntimeException("writeOutputBolt execute: " + e);
			}
		} else {
			if (votesForEos.voteForEos(input.getSourceExecutor())) {
    			/* cluster has no reason to exist!
    			 * we don't need to wait for consensus among multiple writeOutputBolts
    			 * because there is only one */
				context.declareState(STATE.IDLE);
				System.exit(0);
			}
		}
		/* doesn't matter what we return */
		return true;
	}

	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void setRouter(StreamRouter router) {
		// Do nothing
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(myFields);
	}

	@Override
	public Fields getSchema() {
		return myFields;
	}

}

