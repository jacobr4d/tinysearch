package com.jacobr4d.crawler.repository;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class User {
	
	@PrimaryKey(sequence="userId")
	public long userId;
    @SecondaryKey(relate=Relationship.ONE_TO_ONE)
	public String username;
	public String password;

	public User(String username, String password) {
		this.username = username;
		this.password = password;
	}
	
	public User() {} // used for bindings
	
//	public String toString() {
//		return "[" + name + ", "
//				+ password + "]";
//	}
}
