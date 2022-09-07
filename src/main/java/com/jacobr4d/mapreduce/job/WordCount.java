package com.jacobr4d.mapreduce.job;


import java.util.Iterator;

import com.jacobr4d.mapreduce.Context;
import com.jacobr4d.mapreduce.Job;

public class WordCount implements Job {

	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
	public void map(String key, String value, Context context, String sourceExecutor) {
        context.write(value, "1", sourceExecutor);
        /* (term<space>document, 1)*/
    }

	/**
	 * This is a method that lets us call map while recording the StormLite source executor ID.
	 * 
	 */
    public void reduce(String key, Iterator<String> values, Context context, String sourceExecutor) {
        int sum = 0;
        while (values.hasNext()) {
            sum += Integer.parseInt(values.next());
        }
        context.write(key, "" + sum, sourceExecutor);
    }

}
