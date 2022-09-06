package com.jacobr4d.mapreduce.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacobr4d.mapreduce.stormlite.Config;
import com.jacobr4d.mapreduce.stormlite.distributed.WorkerListUtils;

import spark.Spark;

public class MasterServer {
	
	/* Static variables */
	int portNumber;
    ObjectMapper mapper = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
    int activeWorkerThreshold = 30;

    /* Dynamic variables */
	HashMap<String, String> workerStatuses = new HashMap<String, String>();			// workerIpPort -> status as a string
	HashMap<String, Instant> workerStatusTimes = new HashMap<String, Instant>();	// workerIpPort -> instant last status received
	
	final String statusPageHeader = "<head><title>Master</title></head>";
	final String statusPageForm = "<form method=\"POST\" action=\"/submitjob\">" +
		"Job Name: <input type=\"text\" name=\"jobname\"/><br/>" +
    	"Class Name: <input type=\"text\" name=\"classname\"/><br/>" +
    	"Input Directory: <input type=\"text\" name=\"input\"/><br/>" +
    	"Output Directory: <input type=\"text\" name=\"output\"/><br/>" +
    	"Map Threads: <input type=\"text\" name=\"map\"/><br/>" +
    	"Reduce Threads: <input type=\"text\" name=\"reduce\"/><br/>" +
    	"<input type=\"submit\" value=\"Submit\">" +
    	"</form>";

	
	/* masterserver constructor */
	public MasterServer (String portNumber) {
		
		this.portNumber = Integer.valueOf(portNumber);
				
		/* start spark server */
        Spark.port(this.portNumber);
        registerStatus();
        registerWorkerStatus();
        registerSubmitJob();
        registerShutdown();
        
        System.out.println("Master node startup, on port " + portNumber);
	}
	
    /* Route for user to see status */
    public void registerStatus() {
        Spark.get("/status", (req, res) -> {
            StringBuilder html = new StringBuilder("<html>");
            html.append(statusPageHeader);
            html.append("<body>");
            html.append("<p>Jacob Glenn (jacobrad)</p>");
            html.append("<h2>Status</h2>"); 
            html.append("<ul>");
            int workernum = 0;
            for (String worker : getActiveWorkers())
            	html.append("<li>" + workernum++ + ": " + workerStatuses.get(worker) + "</li>");
            html.append("</ul>");
            html.append("<h2>Submit Job</h2>");
            html.append(statusPageForm);
            html.append("</body>");
            html.append("</html>");
            res.type("text/html");
            return (html.toString());
        });
    }
    
    /* Route for workers to send status */
    public void registerWorkerStatus() {
    	Spark.get("/workerstatus", (req, res) -> {
        	workerStatuses.put(req.ip() + ":" + req.queryParams("port"), 
        			"port=" + req.queryParams("port") + ", " +
            		"status=" + req.queryParams("status") + ", " +
            		"job=" + req.queryParams("job") + ", " +
            		"keysRead=" + req.queryParams("keysRead") + ", " +
            		"keysWritten=" + req.queryParams("keysWritten") + ", " +
            		"results=" + req.queryParams("results"));
        	workerStatusTimes.put(req.ip() + ":" + req.queryParams("port"), Instant.now());
        	res.type("text/html");
        	return "Status received";
        });
    }
    
    /* Route for the user to submit a job */
    public void registerSubmitJob() {
    	Spark.post("/submitjob", (req, res) -> {
    		
    		/* Init config with parameters */
            Config config = new Config();
            config.put("job", req.queryParams("jobname"));
            config.put("mapClass", req.queryParams("classname"));
            config.put("reduceClass", req.queryParams("classname"));
            config.put("inputDir", req.queryParams("input"));
            config.put("outputDir", req.queryParams("output"));
            config.put("spoutExecutors", "1");
            config.put("mapExecutors", req.queryParams("map"));
            config.put("reduceExecutors", req.queryParams("reduce"));
            
            /* Init config with worker information */
            List<String> activeWorkers = getActiveWorkers();
            config.put("numWorkers", String.valueOf(activeWorkers.size()));
            config.put("workerList", WorkerListUtils.serialize(activeWorkers));
            int i = 0;
            
            /* Send config to workers */
            for (String worker : activeWorkers) {
		        config.put("workerIndex", String.valueOf(i++));
		        config.put("worker", worker);
				if (Utils.postWithBody("http://" + worker + "/definejob", 
						mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config))
						.getResponseCode() != HttpURLConnection.HTTP_OK)
					throw new RuntimeException("register submit job: failed");
            }
            
            /* Start job on every worker */
			for (String worker: activeWorkers)
				if (Utils.postWithBody("http://" + worker + "/runjob", "").getResponseCode() != 
						HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job execution request failed");
				}
        	res.type("text/html");
    		return "Job submitted";
    	});
    }
    
    /* shutdown master node */
    void shutdown() {
		System.out.println("shutting down workers...");
		for (String worker : getActiveWorkers()) {
			try {
				if (Utils.get("http://" + worker + "/shutdown").getResponseCode() !=
						HttpURLConnection.HTTP_OK)
					System.out.println("shutdown: worker " + worker + " not responding favorably to shutdown. moving on...");
			} catch (IOException e) {
				System.out.println("shutdown: worker " + worker + " not responsive to shutdown. moving on...");
			}
		}
		
		System.out.println("shutting down...");
    	Utils.exitInOneSecond();
    }
    
    /* route for triggering shutdown remotely */
    void registerShutdown() {
    	Spark.get("/shutdown", (req, res) -> {
    		shutdown();
    		return "Shutting down";
    	});
    }
    
	/* Get active workers (posted a status within last 30 seconds) */
	public List<String> getActiveWorkers() {
		List<String> activeWorkers = new ArrayList<String>();
		for(String worker : workerStatusTimes.keySet())
			if (workerStatusTimes.get(worker).isAfter(Instant.now().minusSeconds(activeWorkerThreshold)))
				activeWorkers.add(worker);
		return activeWorkers;
	}
    
    /* Launch a Master */
    public static void main(String[] args) throws IOException {
    	
        if (args.length < 1) {
            System.out.println("Usage: MasterServer [port number]");
            System.exit(1);
        }

        new MasterServer(args[0]);
        System.out.println("Press [Enter] to shut down this node...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		System.exit(0);
    }
}

