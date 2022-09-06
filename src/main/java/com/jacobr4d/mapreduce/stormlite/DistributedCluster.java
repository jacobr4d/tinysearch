/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jacobr4d.mapreduce.stormlite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.jacobr4d.mapreduce.stormlite.bolt.BoltDeclarer;
import com.jacobr4d.mapreduce.stormlite.bolt.IRichBolt;
import com.jacobr4d.mapreduce.stormlite.bolt.OutputCollector;
import com.jacobr4d.mapreduce.stormlite.distributed.SenderBolt;
import com.jacobr4d.mapreduce.stormlite.distributed.WorkerListUtils;
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.spout.IRichSpout;
import com.jacobr4d.mapreduce.stormlite.spout.SpoutOutputCollector;
import com.jacobr4d.mapreduce.stormlite.tasks.ITask;
import com.jacobr4d.mapreduce.stormlite.tasks.SpoutTask;

/**
 * Use multiple threads to simulate a cluster of worker nodes.
 * Hooks to other nodes in a distributed environment.
 * 
 * A thread pool (the executor) executes runnable tasks.  Each
 * task involves calling a nextTuple() or execute() method in
 * a spout or bolt, then routing its tuple to the router. 
 * 
 * @author zives
 *
 */
public class DistributedCluster implements Runnable {
	
	/* static */
	ExecutorService executor = Executors.newFixedThreadPool(1);
			
	/* set at submitTopology */
	public Config config;
	public TopologyContext context;
	Map<String, List<IRichBolt>> bolts = new HashMap<>();
	Map<String, List<IRichSpout>> spouts = new HashMap<>();
	Map<String, StreamRouter> routers = new HashMap<>();

	/* Dynamic */
	Queue<ITask> taskQueue = new ConcurrentLinkedQueue<ITask>();
	AtomicBoolean quit = new AtomicBoolean(false);
	boolean cleanedUp = false;

	
	public DistributedCluster() {
		/* designed so init really happens during submitTopology */
	}
	
	/* sets up topology on cluster and returns execution context */
	public TopologyContext submitTopology(Config config, Topology topo) throws ClassNotFoundException {
		this.config = config;
		this.context = new TopologyContext(this, taskQueue);
		
		bolts.clear();
		spouts.clear();
		routers.clear();
		
		createSpoutInstances(topo, config);
		createBoltInstances(topo, config);
		createRoutes(topo, config);
		
		scheduleSpouts();

		return context;
	}
	
	/**
	 * Starts the worker thread to process events, starting with the spouts.
	 */
	public void startTopology() {
		System.out.println("starting cluster for job named " + config.get("job") + "...");
		
		new Thread(this).start();
	}
	
	/**
	 * The main executor loop uses Java's ExecutorService to schedule tasks.
	 */
	public void run() {
		while (!quit.get()) {
			ITask task = taskQueue.poll();
			if (task == null)
				Thread.yield();
			else {
				executor.execute(task);
			}
		}
		
		System.out.println("shutting down cluster for job named " + config.get("job") + "...");
		System.out.println(executor.shutdownNow().size() + " tasks not executed...");
		
		closeSpoutInstances();
		closeBoltInstances();
		
		cleanedUp = true;
	}
	
	/**
	 * Allocate units of work in the task queue, for each spout
	 */
	private void scheduleSpouts() {
		for (String spoutName: spouts.keySet())
			for (IRichSpout spout: spouts.get(spoutName))
				taskQueue.add(new SpoutTask(spout, taskQueue));
	}
	
	/**
	 * For each spout in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 * @throws ClassNotFoundException 
	 */
	private void createSpoutInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String spoutName: topo.getSpouts().keySet()) {
			spouts.put(spoutName, new ArrayList<IRichSpout>());
			StringIntPair classAndParallelism = topo.getSpout(spoutName);
			for (int i = 0; i < classAndParallelism.getRight(); i++)
				try {
					@SuppressWarnings("deprecation")
					IRichSpout newSpout = (IRichSpout)Class.forName(classAndParallelism.getLeft()).newInstance();
					newSpout.open(config, context, new SpoutOutputCollector(context));
					spouts.get(spoutName).add(newSpout);
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}


	/**
	 * For each bolt in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 * @throws ClassNotFoundException 
	 */
	private void createBoltInstances(Topology topo, Config config) throws ClassNotFoundException {
		for (String boltName: topo.getBolts().keySet()) {
			bolts.put(boltName, new ArrayList<IRichBolt>());
			StringIntPair classAndParallelism = topo.getBolt(boltName);
			int localExecutors = classAndParallelism.getRight();
			for (int i = 0; i < localExecutors; i++)
				try {
					@SuppressWarnings("deprecation")
					IRichBolt newBolt = (IRichBolt)Class.forName(classAndParallelism.getLeft()).newInstance();
					newBolt.prepare(config, context, new OutputCollector(context));
					bolts.get(boltName).add(newBolt);
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}

	/**
	 * Link the output streams to input streams, ensuring that the right kinds
	 * of grouping + routing are accomplished
	 * 
	 * @param topo
	 * @param config
	 */
	private void createRoutes(Topology topo, Config config) {
		// Add destination streams to the appropriate bolts
		for (String boltName: topo.getBolts().keySet()) {
			/* Bolt declarer */
			BoltDeclarer decl = topo.getBoltDeclarer(boltName);
			
			/* Create one router for every bolt name */
			StreamRouter router = decl.getRouter();
			routers.put(boltName, router);
			
			/* Parallelism of bolt */
			int count = bolts.get(boltName).size();
			
			/* Notify router of destinations */
			// Create a bolt for each remote worker, give it the same # of entries
			// as we had locally so round-robin and partitioning will be consistent
			int workerId = 0;
			for (String worker: WorkerListUtils.deserialize(config.get("workerList"))) {
				
				// Create one sender bolt for each node aside from us!
				if (workerId++ != Integer.valueOf(config.get("workerIndex"))) {
					SenderBolt sender = new SenderBolt(worker, boltName);
					sender.prepare(config, context, null);
					for (int i = 0; i < count; i++) {
						router.addRemoteBolt(sender);
					}
					
				// Create one local executor for each node for us!
				} else {
					for (IRichBolt bolt: bolts.get(boltName)) {
						router.addBolt(bolt);
					}
				}
			}
			
			/* Notify router sources of router and set router schema */
			if (topo.getBolts().containsKey(decl.getUpstreamName())) {
				for (IRichBolt bolt: bolts.get(decl.getUpstreamName())) {
					bolt.setRouter(router);
					bolt.declareOutputFields(router);
				}
			} else {
				for (IRichSpout spout: spouts.get(decl.getUpstreamName())) {
					spout.setRouter(router);
					spout.declareOutputFields(router);
				}
			}
			
		}
	}

	/**
	 * For each bolt in the topology, clean up objects
	 * 
	 * @param topo Topology
	 */
	private void closeBoltInstances() {
		for (List<IRichBolt> boltList: bolts.values())
			for (IRichBolt bolt: boltList)
				bolt.cleanup();
	}

	/**
	 * For each spout in the topology, create multiple objects (according to the parallelism)
	 * 
	 * @param topo Topology
	 */
	private void closeSpoutInstances() {
		for (List<IRichSpout> spoutList: spouts.values())
			for (IRichSpout spout: spoutList)
				spout.close();
	}

	
	public void shutdown() {
		quit.getAndSet(true);
		while (!cleanedUp)
			Thread.yield();
	}

	public StreamRouter getStreamRouter(String stream) {
		return routers.get(stream);
	}
}
