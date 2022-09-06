package com.jacobr4d.mapreduce.stormlite.distributed;

import org.eclipse.jetty.util.ConcurrentHashSet;

/**
 * Coordinator for tracking and reaching consensus
 * 
 * Tracks both who voted and how many votes were recorded.
 * If someone votes twice, there is an exception.  If enough
 * votes are received to pass a specified threshold, we assume consensus
 * is reached.
 *
 */
public class ConsensusTracker {
	
	/**
	 * Track the set of voters
	 */
	ConcurrentHashSet<String> voters = new ConcurrentHashSet<String>(); 
	
	int votesNeeded;
	
	public ConsensusTracker(int votesNeeded) {
		this.votesNeeded = votesNeeded;
	}
	
	/**
	 * Add another vote towards consensus.
	 * @param voter Optional ID of the node / executor that voted, for tracking
	 * if anyone is voting more than once!
	 * 
	 * @return true == we have enough votes for consensus end-of-stream.
	 *         false == we don't yet have enough votes.
	 */
	public boolean voteForEos(String voter) {
		
		if (voter == null || voter.isEmpty())
			throw new RuntimeException("consensustracker: cannot vote anonymously");
		
		int votes;
		synchronized (voters) {
			voters.add(voter);
			votes = voters.size();
		}
		
		return (votes >= votesNeeded);
	}
}
