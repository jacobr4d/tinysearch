package com.jacobr4d.crawler.utils;

import java.util.Base64;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HttpUtils {

	
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
