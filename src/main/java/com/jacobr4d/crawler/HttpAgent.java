package com.jacobr4d.crawler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.HttpsURLConnection;

import com.jacobr4d.crawler.utils.RobotsInfo;
import com.jacobr4d.crawler.utils.URLInfo;

public class HttpAgent {
	
	URLConnection urlCon;
	HttpURLConnection con;
	
	   
    /* Make head request with no data */
	public HttpURLConnection head(URLInfo url) throws IOException {
		urlCon = (new URL(url.toString())).openConnection();
		con = url.isSecure() ? (HttpsURLConnection) urlCon : (HttpURLConnection) urlCon;
		con.setRequestMethod("HEAD");
		con.setRequestProperty("User-Agent", "cis455crawler");
		con.setConnectTimeout(5000); /* 5s timeout */
		con.setReadTimeout(5000);
    	return con;
    }	
    
    /* Make get request with no data */
	public HttpURLConnection get(URLInfo url) throws IOException {
		urlCon = (new URL(url.toString())).openConnection();
		con = url.isSecure() ? (HttpsURLConnection) urlCon : (HttpURLConnection) urlCon;
		con.setRequestProperty("User-Agent", "cis455crawler");
		con.setConnectTimeout(5000); /* 5s timeout */
		con.setReadTimeout(5000);
    	return con;
    }
	
	public RobotsInfo getRobotsInfo(URLInfo url) throws IOException {
		con = get(url.copy().setFilePath("/robots.txt"));		
		if (con.getResponseCode() != HttpURLConnection.HTTP_OK) {
			return new RobotsInfo();
		} else {
			String content = new String(con.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			return new RobotsInfo(content); 
		}
	}

}
