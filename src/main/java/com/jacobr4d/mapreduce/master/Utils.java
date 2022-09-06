package com.jacobr4d.mapreduce.master;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Utils {
	
	/* Helpful for shutting down */
	public static void exitInOneSecond() {
    	Runnable exitSequence = new Runnable() {
    		public void run() {
    			System.exit(0);
    		}
    	};
    	Executors.newScheduledThreadPool(1).schedule(exitSequence, 1, TimeUnit.SECONDS);
    }
	
    /* Make post request with data in the body */
    public static HttpURLConnection postWithBody(String urlString, String body) throws IOException {
		URL url = new URL(urlString);		
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "application/json");
		OutputStream os = conn.getOutputStream();
		os.write(body.getBytes());
		os.flush();
		return conn;
    }
    
    /* Make get request with no data */
    public static HttpURLConnection get(String urlString) throws IOException {
    	URL url = new URL(urlString);
    	return (HttpURLConnection) url.openConnection();
    }

}
