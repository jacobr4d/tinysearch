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
import com.jacobr4d.mapreduce.stormlite.routers.StreamRouter;
import com.jacobr4d.mapreduce.stormlite.spout.IRichSpout;
import com.jacobr4d.mapreduce.stormlite.spout.SpoutOutputCollector;
import com.jacobr4d.mapreduce.stormlite.tasks.ITask;
import com.jacobr4d.mapreduce.stormlite.tasks.SpoutTask;

/**
 * Use multiple threads to simulate a cluster of worker nodes.
 * Emulates a distributed environment.
 * 
 * A thread pool (the executor) executes runnable tasks.  Each
 * task involves calling a nextTuple() or execute() method in
 * a spout or bolt, then routing its tuple to the router. 
 * 
 * @author zives
 *
 */
public class LocalCluster implements Runnable {
	
	/* Static after Setup */
	public String topologyName= "None";
	public TopologyContext context = null;
	ExecutorService executor = Executors.newFixedThreadPool(1);
	Map<String, List<IRichBolt>> bolts = new HashMap<>();
	Map<String, List<IRichSpout>> spouts = new HashMap<>();

	/* Dynamically changing fields */
	Queue<ITask> taskQueue = new ConcurrentLinkedQueue<>();
	AtomicBoolean quit = new AtomicBoolean(false);
	boolean cleanedUp = false;
	

	public void submitTopology(String name, Config config, Topology topo) throws ClassNotFoundException {
		topologyName = name;
//		context = new TopologyContext(this, topo, taskQueue);
		createSpoutInstances(topo, config);
		scheduleSpouts();
		createBoltInstances(topo, config);
		createRoutes(topo, config);
		// Put the run method in a background thread
		new Thread(this).start();
	}
	
	public void run() {
		while (!quit.get()) {
			Runnable task = taskQueue.poll();
			if (task == null)
				Thread.yield();
			else {
				executor.execute(task);
			}
		}	
//		System.out.println("Task scheduler stopped");
		System.out.println("Thread pool shut down: " + executor.shutdownNow().size() + " tasks not executed");
		closeSpoutInstances();
		closeBoltInstances();
//		System.out.println("Spouts and bolts closed");
//        System.out.println("Topology shutdown");
		cleanedUp = true;
	}
	
	private void scheduleSpouts() {
		for (String key: spouts.keySet())
			for (IRichSpout spout: spouts.get(key)) {
				taskQueue.add(new SpoutTask(spout, taskQueue));
			}
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
			for (int i = 0; i < classAndParallelism.getRight(); i++)
				try {
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
		for (String boltName: topo.getBolts().keySet()) {
			/* Bolt declarer */
			BoltDeclarer decl = topo.getBoltDeclarer(boltName);
			
			/* Create one router for every boltName */
			StreamRouter router = decl.getRouter();
			String upStreamName = decl.getUpstreamName();
			
			/* Notify router of its destinations */
			for (IRichBolt bolt: bolts.get(boltName))
				router.addBolt(bolt);
			
			/* Notify router sources of router and set router schema */
			if (topo.getBolts().containsKey(upStreamName)) {
				for (IRichBolt bolt: bolts.get(upStreamName)) {
					bolt.setRouter(router);
					bolt.declareOutputFields(router);
				}
			} else {
				for (IRichSpout spout: spouts.get(upStreamName)) {
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

	/**
	 * Shut down the cluster
	 * 
	 * @param string
	 */
	public void shutdown() {
		quit.getAndSet(true);
		while (!cleanedUp)
			Thread.yield();
	}

}
