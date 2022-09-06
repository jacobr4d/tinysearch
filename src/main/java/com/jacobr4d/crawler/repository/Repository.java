package com.jacobr4d.crawler.repository;

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;

import com.jacobr4d.crawler.utils.*;


public class Repository {
	
	public String envPath;
	public Environment env;
	EntityStore store;
	PrimaryIndex<Long, Document> documentIndex;
	SecondaryIndex<String, Long, Document> documentByURL;
	PrimaryIndex<Long, User> userIndex;
	SecondaryIndex<String, Long, User> userByName;
	PrimaryIndex<Long, Channel> channelIndex;
	SecondaryIndex<String, Long, Channel> channelByName;
	PrimaryIndex<Long, DocumentMatchesChannel> matchIndex;
	SecondaryIndex<String, Long, DocumentMatchesChannel> matchByChannel;
	
	public Repository(String envPath) {
		this.envPath = envPath;
		
		/* make storage dir if doesn't exist */
		File dir = new File(envPath);
		if (!dir.exists()) {
			if (!dir.mkdir())
				throw new RuntimeException("berkeleyDatabase: unable to make dir " + dir);
		}
		
		env = new Environment((new File(envPath)), new EnvironmentConfig().setAllowCreate(true));
		store = new EntityStore(env, "store", new StoreConfig().setAllowCreate(true));
		
		documentIndex = store.getPrimaryIndex(Long.class, Document.class);
		documentByURL = store.getSecondaryIndex(documentIndex, String.class, "url");
		userIndex = store.getPrimaryIndex(Long.class, User.class);
		userByName = store.getSecondaryIndex(userIndex, String.class, "username");
		channelIndex = store.getPrimaryIndex(Long.class, Channel.class);
		channelByName = store.getSecondaryIndex(channelIndex, String.class, "channelName");
		matchIndex = store.getPrimaryIndex(Long.class, DocumentMatchesChannel.class);
		matchByChannel = store.getSecondaryIndex(matchIndex, String.class, "channelName");
	}

	public void close() {
		store.close();
	    env.close();
	}
	
	public long getNumChannels() {
		return channelIndex.count();
	}
	public long getNumMatches() {
		return matchIndex.count();
	}

	public void putUser(String name, String password) {
		userIndex.putNoReturn(new User(name, HashUtils.sha(password)));
	}
	
	public User getUser(String name) {
		return userByName.get(name);
	}
	
	public boolean validate(String name, String password) {
		User user = getUser(name);
		return user != null && user.password.equals(HashUtils.sha(password)); 
	}

	public void putDocument(Document document) {
		documentIndex.putNoReturn(document);
	}

	public Document getDocument(URLInfo urlInfo) {
		return documentByURL.get(urlInfo.toString());
	}

	public EntityCursor<String> documentCursor() {
		return documentByURL.keys();
	}
	
	public void putChannel(Channel channel) {
		channelIndex.putNoReturn(channel);
	}
	
	public Channel getChannel(String name) {
		return channelByName.get(name);
	}
	
	public EntityCursor<String> channelCursor() {
		return channelByName.keys();
	}
	
	public void putMatchIdempotent(String url, String channelName) {
		
		/* dont insert if it exists */
		EntityCursor<DocumentMatchesChannel> matchCursor = documentMatchesChannelCursor(channelName);
		try {
			for (DocumentMatchesChannel match = matchCursor.first(); match != null; match = matchCursor.next()) {
				if (match.documentURL.equals(url))
					return;
			}
		} finally {
			matchCursor.close();
		}
		
		/* insert */
		DocumentMatchesChannel match = new DocumentMatchesChannel();
		match.documentURL = url;
		match.channelName = channelName;
		matchIndex.putNoReturn(match);
	}
	
	
	/* cursor over channel names (not unique, which is weird) 
	 * best to use this with nextNoDup() then another loop instide */
	public EntityCursor<String> channelsWithMatchesCursor() {
		return matchByChannel.keys();
	}
	
	/* cursor over matches of form (channelName, *) */
	public EntityCursor<DocumentMatchesChannel> documentMatchesChannelCursor(String channelName) {
		return matchByChannel.subIndex(channelName).entities();
	}


}
