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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import com.jacobr4d.mapreduce.stormlite.tasks.ITask;

/**
 * Information about the execution of a topology, including
 * the stream routers
 * 
 * @author zives
 *
 */
public class TopologyContext {
	
	/* definition */
	public static enum STATE {INIT, MAPPING, REDUCING, IDLE};

	/* static after init */
	DistributedCluster cluster;
	Queue<ITask> taskQueue;
	
	/* dynamic */
	public STATE state = STATE.INIT;
	AtomicInteger keysRead = new AtomicInteger(0);
	AtomicInteger keysWritten = new AtomicInteger(0);
	List<String> sampleResults = new ArrayList<String>();
	
	
	public TopologyContext(DistributedCluster cluster, Queue<ITask> taskQueue) {
		this.cluster = cluster;
		this.taskQueue = taskQueue;
	}
	
	/* Declare state, and shift there if not shifted yet */
	public void declareState(STATE state) {
		synchronized (this.state) {
			if (state == STATE.MAPPING) {
				if (this.state != STATE.MAPPING) {
					setState(STATE.MAPPING);
					resetKeysRead();
					resetKeysWritten();
				}
			} else if (state == STATE.REDUCING) {
				if (this.state != STATE.REDUCING) {
					setState(STATE.REDUCING);
					resetKeysRead();
					resetKeysWritten();
				}
			} if (state == STATE.IDLE) {
				if (this.state != STATE.IDLE) {
					setState(STATE.IDLE);
					resetKeysRead();
					resetKeysWritten();
					cluster.shutdown();
				}
			}
			
		}
	}
	
	public void setState(STATE state) {
		this.state = state;
	}
	
	public STATE getState() {
		return state;
	}
	
	public void resetKeysRead() {
		keysRead.set(0);
	}
	
	public void incrementKeysRead() {
		keysRead.incrementAndGet();
	}
	
	public int keysRead() {
		return keysRead.intValue();
	}
	
	public void resetKeysWritten() {
		keysWritten.set(0);
	}
	
	public void incrementKeysWritten() {
		keysWritten.incrementAndGet();
	}
	
	public int keysWritten() {
		return keysWritten.intValue();
	}
	
	public void logOutput(String token) {
		if (sampleResults.size() < 100)
			sampleResults.add(token);
	}
	
	public List<String> getSampleResults() {
		return sampleResults;
	}
	
	public void addStreamTask(ITask next) {
		taskQueue.add(next);
	}

	
}
