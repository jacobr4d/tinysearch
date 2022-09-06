package com.jacobr4d.mapreduce.stormlite.tasks;

public interface ITask extends Runnable {
	public String getStream();
}
