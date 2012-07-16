package com.bizosys.hsearch.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.log4j.Logger;

/**
 * Does MD5 hashing
 * @author karan
 *
 */
public class Hash {
	private static Logger LOG = Logger.getLogger(Hash.class);
    private static final char[] hexChars =
	{'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};	

	public static String createHex(String key, String value ) {
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ex) {
			LOG.fatal("MD5 Hash package is not loaded");
			throw new RuntimeException("MD5 Hash package is not loaded", ex);
		}
		
		if ( LOG.isDebugEnabled()) {
			//LOG.debug("key Length =" + key.length() + " and value=" + value);
		}
		md5.update(key.getBytes());
		md5.update(value.getBytes());
		byte[] contextDigest = md5.digest();
		
		StringBuffer buffer = new StringBuffer();
		hexStringFromBytes(contextDigest,buffer);
		
		return buffer.toString();
	}

	 private static void hexStringFromBytes(byte[] b, StringBuffer hex) { 
		 int msb; 
		 int lsb = 0; 
	     int i; 
	  
	     // MSB maps to idx 0 
	     for (i = 0; i < b.length; i++) { 
	    	 msb = ((int)b[i] & 0x000000FF) / 16; 
	    	 lsb = ((int)b[i] & 0x000000FF) % 16; 
	    	 hex.append(hexChars[msb]).append(hexChars[lsb]); 
	     } 
	 } 
    
}
