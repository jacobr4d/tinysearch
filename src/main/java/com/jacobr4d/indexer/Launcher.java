package com.jacobr4d.indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jacobr4d.mapreduce.master.MasterServer;
import com.jacobr4d.mapreduce.worker.WorkerServer;

public class Launcher {
	private static final Logger logger = LogManager.getLogger(Launcher.class);
	
	public static void main(String args[]) throws IOException {
				
		if (args.length != 4) {
            logger.info("Usage: Launcher [jobName] [jobPath] [inputPath] [outputPath]");
            System.exit(1);
        }

		new MasterServer("45555");
        new WorkerServer("8000", "localhost:45555", "output/mapreduce");
        
        /* send submit job to master */
        String data = "jobname=" + args[0] + "&classname=" + args[1] + "&input=" + args[2] + "&output=" + args[3] + "&map=1&reduce=1";
        System.out.println(data);
        HttpURLConnection con = (HttpURLConnection) new URL("http://localhost:45555/submitjob").openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.getOutputStream().write(data.getBytes("UTF-8"));
        con.getInputStream();
        
		(new BufferedReader(new InputStreamReader(System.in))).readLine();
		System.exit(0);
	}
	
	
}
