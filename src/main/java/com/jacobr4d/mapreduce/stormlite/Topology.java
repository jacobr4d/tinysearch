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

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.jacobr4d.mapreduce.stormlite.bolt.BoltDeclarer;
import com.jacobr4d.mapreduce.stormlite.bolt.IRichBolt;
import com.jacobr4d.mapreduce.stormlite.distributed.StringIntPairKeyDeserializer;
import com.jacobr4d.mapreduce.stormlite.spout.IRichSpout;
import com.jacobr4d.mapreduce.stormlite.tuple.Fields;

public class Topology {

	/**
	 * NAME, CLASS, PARALLELISM
	 */
	Map<String,StringIntPair> spouts = new HashMap<>();
	
	/**
	 * NAME, CLASS, PARALLELISM
	 */
	Map<String, StringIntPair> bolts = new HashMap<>();
	
	/**
	 * NAME, BOLTDECLARER
	 */
	Map<String, BoltDeclarer> boltGrouping = new HashMap<>();
	
	/**
	 * Bolts have multiple inputs connected to spouts (or other bolts)
	 */
	@JsonDeserialize(keyUsing = StringIntPairKeyDeserializer.class)
	Map<StringIntPair, String> boltConnectors = new HashMap<>();
	
	/**
	 * Each Stream has a set of fields, i.e., a schema
	 */
	Map<String, Fields> streamSchemas = new HashMap<>();


	public Map<String, StringIntPair> getSpouts() {
		return spouts;
	}
	
	public StringIntPair getSpout(String key) throws ClassNotFoundException {
		StringIntPair entry = spouts.get(key);
		return entry;
	}

	public void setSpouts(String name, Class<? extends IRichSpout> spoutClass, Integer parallel) {
		this.spouts.put(name, new StringIntPair(spoutClass.getName(), Integer.valueOf(parallel)));
	}

	public Map<String, StringIntPair> getBolts() {
		return bolts;
	}
	
	public StringIntPair getBolt(String key) throws ClassNotFoundException {
		StringIntPair entry = bolts.get(key);
		return entry;
	}

	public void setBolts(String bolt, Class<? extends IRichBolt> boltClass, Integer parallel) {
		this.bolts.put(bolt, new StringIntPair(boltClass.getName(), Integer.valueOf(parallel)));
	}

	public Map<StringIntPair, String> getBoltConnectors() {
		return boltConnectors;
	}

	public void setBoltConnectors(Map<StringIntPair, String> boltConnectors) {
		this.boltConnectors = boltConnectors;
	}

	public Map<String, Fields> getStreamSchemas() {
		return streamSchemas;
	}

	public void setStreamSchemas(Map<String, Fields> streamSchemas) {
		this.streamSchemas = streamSchemas;
	}

	public Map<String, BoltDeclarer> getBoltGrouping() {
		return boltGrouping;
	}

	public void setBoltGrouping(String streamID, BoltDeclarer boltDeclarer) {
		this.boltGrouping.put(streamID, boltDeclarer);
	}

	public BoltDeclarer getBoltDeclarer(String stream) {
		return getBoltGrouping().get(stream);
	}
	
}
