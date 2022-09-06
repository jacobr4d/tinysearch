package com.jacobr4d.mapreduce.stormlite.distributed;

import java.util.Arrays;
import java.util.List;

public class WorkerListUtils {
	
	/**
	 * Get the list of workers
	 * 
	 * @param config
	 * @return
	 */
	
	public static List<String> deserialize(String workerList) {
		return Arrays.asList(workerList.substring(1, workerList.length() - 1).split(", "));
	}
	
	public static String serialize(List<String> workerList) {
		return workerList.toString();
	}
	
//	public static String[] getWorkers(Map<String, String> config) {
//		String list = config.get("workerList");
//		if (list.startsWith("["))
//			list = list.substring(1);
//		if (list.endsWith("]"))
//			list = list.substring(0, list.length() - 1);
//		
//		String[] servers = list.split(",");
//		
//		String[] ret = new String[servers.length];
//		int i = 0;
//		for (String item: servers)
//			if (!item.startsWith("http"))
//				ret[i++] = "http://" + item;
//			else
//				ret[i++] = item;
//			
//		return ret;
//	}
}
