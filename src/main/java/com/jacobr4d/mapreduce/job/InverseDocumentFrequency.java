package com.jacobr4d.mapreduce.job;

import java.util.Iterator;

import com.jacobr4d.indexer.index.Index;
import com.jacobr4d.mapreduce.Context;
import com.jacobr4d.mapreduce.Job;

public class InverseDocumentFrequency implements Job {

	int n = 1000000; //repo size
	Index index;
	
	public InverseDocumentFrequency() {
		n = Integer.valueOf(System.getenv("REPO_SIZE"));
	}
	
	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
	public void map(String key, String value, Context context, String sourceExecutor) {
		// word<space>url<spcae>tf
        String[] toks = value.split("\\s+");
        context.write(toks[0], "1", sourceExecutor);
    }

	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
    public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor) {
    	// word<space>1
    	int nt = 0;
        while (values.hasNext()) {
            nt += Integer.parseInt(values.next());
        }
        context.write(key, String.valueOf(Math.log( 1.0 * n / (1 + nt) ) + 1), sourceExecutor);
    }

}
