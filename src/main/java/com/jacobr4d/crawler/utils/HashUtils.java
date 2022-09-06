package com.jacobr4d.crawler.utils;

import org.apache.commons.codec.digest.DigestUtils;

public class HashUtils {

	/* md5 hash string of bytes */
	public static String md5(byte[] contents) {
		return DigestUtils.md5Hex(contents).toUpperCase();
	}
	
	/* sha hash string of string */
	public static String sha(String password) {
		return DigestUtils.sha256Hex(password).toUpperCase();
	}
}
