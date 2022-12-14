package com.jacobr4d.mapreduce.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacobr4d.mapreduce.master.Utils;
import com.jacobr4d.mapreduce.stormlite.Config;
import com.jacobr4d.mapreduce.stormlite.DistributedCluster;
import com.jacobr4d.mapreduce.stormlite.Topology;
import com.jacobr4d.mapreduce.stormlite.TopologyBuilder;
import com.jacobr4d.mapreduce.stormlite.bolt.MapBolt;
import com.jacobr4d.mapreduce.stormlite.bolt.ReduceBolt;
import com.jacobr4d.mapreduce.stormlite.bolt.WriteOutputBolt;
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.spout.ReadInputSpout;
import com.jacobr4d.mapreduce.stormlite.tuple.Fields;
import com.jacobr4d.mapreduce.stormlite.tuple.Tuple;

import spark.Service;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
public class WorkerServer {
	private static final Logger logger = LogManager.getLogger(WorkerServer.class);

	
	/* Static after initialization */
	int pingStatusFequencySeconds = 10;
	public int port;
    public String masterIpPort;
    public String storageDir;
    
    ObjectMapper om = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

    /* Dynamic */
    DistributedCluster cluster = null;
    
    
    public WorkerServer(String port, String masterIpPort, String storageDir) throws MalformedURLException {
    	
    	this.port = Integer.valueOf(port);
    	this.masterIpPort = masterIpPort;
    	this.storageDir = storageDir;
    	    	    	
    	/* initialize routes for spark server */
		Service server = Service.ignite().port(this.port).threadPool(10);
		server.post("/definejob", (req, res) -> {
            try {            	
                Config config = om.readValue(req.body(), Config.class);
            	
                /* Put worker-specific things in config */
                config.put("storageDir", storageDir);
                config.put("masterIpPort", masterIpPort);
                                
                /* Renew cluster */
            	cluster = new DistributedCluster();

                /* Make and submit topology */
                cluster.submitTopology(config, mapReduceTopology(config));
                
                return "Job defined";
            } catch (Exception e) {
            	e.printStackTrace();
                logger.info("/definejob: " + e);
                res.status(500);
                return e.getMessage();
            } 
        });
		server.post("/runjob", (req, res) -> {
        	cluster.startTopology();
        	return "Job Started";
        });
		server.post("/pushdata/:boltName", (req, res) -> {
            try {
            	/* Read tuple from body */
                Tuple tuple = om.readValue(req.body(), Tuple.class);

                /* Execute tuple locally on appropriate router */
                StreamRouter router = cluster.getStreamRouter(req.params(":boltName"));
                if (tuple.isEndOfStream())
                	router.executeEndOfStreamLocally(cluster.context, tuple.getSourceExecutor());
                else
                	router.executeLocally(tuple, cluster.context, tuple.getSourceExecutor());
                
                return "OK";
            } catch (IOException e) {
                logger.info("/pushdata: " + e);
                res.status(500);
                return e.getMessage();
            }

        });
        
        /* start pinging master */
        initiateMasterPingRoutine(pingStatusFequencySeconds);
        server.get("/shutdown", (req, res) -> {
			logger.info("shutting down...");
    		Utils.exitInOneSecond();
    		return "Shutting down";
    	});
        logger.info("Worker node startup, on port " + port);
    }
    
    void initiateMasterPingRoutine(int frequencySeconds) {
    	Runnable updateStatus = new Runnable() {
    		public void run() {
    			String urlString;
    			try {
					if (cluster == null) {
						urlString = 
							"http://" + masterIpPort + "/workerstatus?" +
							"port=" + port + "&" +
		            		"status=" + "IDLE" + "&" +
		            		"job=" + "None" + "&" +
		            		"keysRead=" + "0" + "&" +
		            		"keysWritten=" + "0" + "&" +
		            		"results=" + URLEncoder.encode("[]", "UTF-8");
					} else {
						urlString = "http://" + masterIpPort + "/workerstatus?" +
							"port=" + port + "&" +
		            		"status=" + cluster.context.state.name() + "&" +
		            		"job=" + cluster.config.get("mapClass") + "&" +
		            		"keysRead=" + cluster.context.keysRead() + "&" +
		            		"keysWritten=" + cluster.context.keysWritten() + "&" +
		            		"results=" + URLEncoder.encode(cluster.context.getSampleResults().toString(), "UTF-8");
					}

    				if (Utils.get(urlString).getResponseCode() != HttpURLConnection.HTTP_OK) {
        				logger.info("ping master: master not happy");
    				}
    			} catch (Exception e) {
    				logger.info("ping master: " + e);
    			}
    		}
    	};
    	Executors.newScheduledThreadPool(1).scheduleAtFixedRate(updateStatus, 0, frequencySeconds, TimeUnit.SECONDS);
    }
    
    /* Make topology for mapreduce computation on a worker given complete config */
    public static Topology mapReduceTopology(Config config) {
    	
    	ReadInputSpout spout = new ReadInputSpout();
        MapBolt mapper = new MapBolt();
        ReduceBolt reducer = new ReduceBolt();
        WriteOutputBolt printer = new WriteOutputBolt();
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("WORDSPOUT", spout, Integer.valueOf(config.get("spoutExecutors"))); 
        builder.setBolt("MAPBOLT", mapper, Integer.valueOf(config.get("mapExecutors")))
        	.fieldsGrouping("WORDSPOUT", new Fields("key"));
        builder.setBolt("REDUCEBOLT", reducer, Integer.valueOf(config.get("reduceExecutors")))
        	.fieldsGrouping("MAPBOLT", new Fields("key"));
        builder.setBolt("PRINTBOLT", printer, 1)			// how we handle exit relies on one printer per worker currently
        	.firstGrouping("REDUCEBOLT");
        
        return builder.createTopology();
    }

    /* launch worker */
    public static void main(String args[]) throws IOException, InterruptedException {
    	
        if (args.length < 3) {
            logger.info("Usage: WorkerServer [portnumber] [master ip]:[master port] [storagedir]");
            System.exit(1);
        }

        new WorkerServer(args[0], args[1], args[2]);
        
        logger.info("Press [Enter] to shut down this node...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		System.exit(0);
    }
}
