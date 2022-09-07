package com.jacobr4d.mapreduce.job;

import java.util.Iterator;

import com.jacobr4d.mapreduce.Context;
import com.jacobr4d.mapreduce.Job;

public class TermFrequency implements Job {

	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
	public void map(String key, String value, Context context, String sourceExecutor) {
        String[] toks = value.split("\\s+");
        context.write(toks[0] + " " + toks[1], "1", sourceExecutor);
    }

	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
    public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor) {
        int sum = 0;
        while(values.hasNext()) {
            sum += Integer.parseInt(values.next());
        }
        context.write(key, "" + Math.log(1 + sum), sourceExecutor);
    }

}
