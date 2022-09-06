package com.jacobr4d.crawler.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

public class HttpUtils {
    
    /* Make head request with no data */
	public static HttpURLConnection head(URLInfo url) throws IOException {
		URLConnection urlCon = (new URL(url.toString())).openConnection();
		HttpURLConnection con = url.isSecure() ? (HttpsURLConnection) urlCon : (HttpURLConnection) urlCon;
		con.setRequestMethod("HEAD");
		con.setRequestProperty("User-Agent", "cis455crawler");
		con.setConnectTimeout(5000); /* 5s timeout */
		con.setReadTimeout(5000);
    	return con;
    }	
    
    /* Make get request with no data */
	public static HttpURLConnection get(URLInfo url) throws IOException {
		URLConnection urlCon = (new URL(url.toString())).openConnection();
		HttpURLConnection con = url.isSecure() ? (HttpsURLConnection) urlCon : (HttpURLConnection) urlCon;
		con.setRequestProperty("User-Agent", "cis455crawler");
		con.setConnectTimeout(5000); /* 5s timeout */
		con.setReadTimeout(5000);
    	return con;
    }

	
	/* Helpful for shutting down */
	public static void exitInOneSecond() {
    	Runnable exitSequence = new Runnable() {
    		public void run() {
    			System.exit(0);
    		}
    	};
    	Executors.newScheduledThreadPool(1).schedule(exitSequence, 1, TimeUnit.SECONDS);
    }
	
	/* channel names must be nonempty alphanumeric */
	public static boolean isValidChannelName(String channelName) {
		return channelName.matches("^[_-[.]a-zA-Z0-9]+$");
	}
	
	/* xpath spec given in assignment */
	public static boolean isValidXPath(String pattern) {
		String pattern1 = pattern.replaceAll("[\\s+](?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", ""); // remove whitespace not in quotes
		return pattern1.matches("(/[_-[.]a-zA-Z0-9]+)+(\\[(text\\(\\)=\"[^\"]+\"|contains\\(text\\(\\),\"[^\\\"]+\"\\))\\])?");
	}

	public static String encodeURL(String url) {
		return new String(Base64.getUrlEncoder().encode(url.getBytes()));
	}
	public static String decodeURL(String url) {
		return new String(Base64.getUrlDecoder().decode(url.getBytes()));
	}

}
