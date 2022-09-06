package com.jacobr4d.mapreduce.stormlite.distributed;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacobr4d.mapreduce.stormlite.OutputFieldsDeclarer;
import com.jacobr4d.mapreduce.stormlite.TopologyContext;
import com.jacobr4d.mapreduce.stormlite.bolt.IRichBolt;
import com.jacobr4d.mapreduce.stormlite.bolt.OutputCollector;
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.tuple.Fields;
import com.jacobr4d.mapreduce.stormlite.tuple.Tuple;

/**
 * This is a virtual bolt that is used to route data to the WorkerServer
 * on a different worker.
 * 
 * @author zives
 *
 */
public class SenderBolt implements IRichBolt {

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
	Fields schema = new Fields("key", "value");
	
	String boltName;
	String address;
    ObjectMapper mapper = new ObjectMapper();
	URL url;
	
	TopologyContext context;
	
	boolean isEndOfStream = false;
	

    public SenderBolt(String address, String boltName) {
    	this.boltName = boltName;
    	this.address = address;
    }
    
	/**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        this.context = context;
		try {
			url = new URL("http://" + address + "/pushdata/" + boltName);
		} catch (MalformedURLException e) {
			throw new RuntimeException("Unable to create remote URL");
		}
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public synchronized boolean execute(Tuple input) {
    		try {
				send(input);
			} catch (IOException e) {
				e.printStackTrace();
			}
    		return true;
    }
    
    /**
     * Sends the data along a socket
     * 
     * @param stream
     * @param tuple
     * @throws IOException 
     */
    private void send(Tuple tuple) throws IOException {
//    	isEndOfStream = tuple.isEndOfStream();
    	
//		 log.debug("Sender is routing " + tuple.toString() + " from " + tuple.getSourceExecutor() + " to " + address + "/" + boltName);
		
    	if (sendPostWithBody(url, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(tuple))
    		.getResponseCode() != HttpURLConnection.HTTP_OK)
			throw new RuntimeException("sender bolt: tuple couldn't be sent");
		
//		conn.disconnect();
    }
    
    /* Make post request with data in the body */
    public HttpURLConnection sendPostWithBody(URL url, String body) throws IOException {
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "application/json");
		OutputStream os = conn.getOutputStream();
		os.write(body.getBytes());
		os.flush();
		return conn;
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
     * Used for debug purposes, shows our executor/operator's unique ID
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
		// NOP for this, since its destination is a socket
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
}
