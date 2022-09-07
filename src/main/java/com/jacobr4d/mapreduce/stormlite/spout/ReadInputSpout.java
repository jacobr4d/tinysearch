package com.jacobr4d.mapreduce.stormlite.spout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.jacobr4d.mapreduce.stormlite.OutputFieldsDeclarer;
import com.jacobr4d.mapreduce.stormlite.TopologyContext;
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.tuple.Fields;
import com.jacobr4d.mapreduce.stormlite.tuple.Values;

public class ReadInputSpout implements IRichSpout {

	/* static */
	Fields schema = new Fields("key", "value");
    String executorId = UUID.randomUUID().toString();

    /* set at open */
    SpoutOutputCollector collector;
    BufferedReader reader;
    
    /* dynamic */
    int inx = 0;
    boolean sentEos = false;

    
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        
        if (!config.containsKey("storageDir"))
        	throw new RuntimeException("readInputBolt doesn't know storage dir");
        
        if (!config.containsKey("inputFile"))
        	throw new RuntimeException("readInputBolt doesn't know inputFile");
    	
    	this.collector = collector;
        
        try {
            reader = new BufferedReader(new FileReader(config.get("storageDir") + "/" + config.get("inputFile")));
        } catch (Exception e) {
            throw new RuntimeException("inputfilespout: open: " + e);
        }
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
        if (reader != null)
            try {
                reader.close();
            } catch (IOException e) {
    			throw new RuntimeException("readInputSpout close: " + e);
            }
    }

    @Override
    public synchronized boolean nextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                this.collector.emit(new Values<Object>(String.valueOf(inx++), line), getExecutorId());
                Thread.yield();
                return true;
            } else if (!sentEos) {
                this.collector.emitEndOfStream(getExecutorId());
                sentEos = true;
                return false;
            }
        } catch (Exception e) {
			throw new RuntimeException("readInputSpout nextTuple: " + e);
        }
        /* only get here if something bad happened */
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }


    @Override
    public String getExecutorId() {
        return executorId;
    }


    @Override
    public void setRouter(StreamRouter router) {
        this.collector.setRouter(router);
    }

}
