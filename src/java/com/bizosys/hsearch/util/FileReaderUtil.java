/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * Various file related utility operations.
 * @author karan
 *
 */
public class FileReaderUtil {
	public static Logger l = Logger.getLogger(FileReaderUtil.class.getName());

	/**
	 * Reads the complete file which is in the classpath
	 * line by line. If the line has no text or commented out
	 * it leaves the line. Pack all these lines in a list and gives back.
	 * @param fileName	File name
	 * @return	List of lines
	 * @throws ApplicationFault
	 */
	public static final List<String> toLines(String fileName) throws  ApplicationFault, SystemFault {
		
		File aFile = getFile(fileName);
		BufferedReader reader = null;
		InputStream stream = null;
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			List<String> lines = new ArrayList<String>();
			String line = null;
			while((line=reader.readLine())!=null) {
				if (line.length() == 0) continue;
				char first=line.charAt(0);
				switch (first) {
					case ' ' : case '\n' : case '#' :  // skip blank & comment lines
					continue;
				}
				lines.add(line);	
			}
			return lines;
		} catch (Exception ex) {
			l.fatal("util.FileReaderUtil", ex);			
			throw new SystemFault(ex);
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {l.warn("util.FileReaderUtil", ex);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {l.warn("util.FileReaderUtil", ex);}
		}
	}
	
	public static int getLineCount(String fileName) throws ApplicationFault, SystemFault {
		
		File aFile = getFile(fileName);
		BufferedReader reader = null;
		InputStream stream = null;
		int totalLine = 0; 
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			while((reader.readLine())!=null) {
				totalLine++;
			}
			return totalLine; 
		} catch (Exception ex) {
			l.fatal("util.FileReaderUtil", ex);			
			throw new SystemFault(ex);
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {l.warn("util.FileReaderUtil", ex);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {l.warn("util.FileReaderUtil", ex);}
		}
	}	
	
	/**
	 * Give the file in name values
	 * @param lines	
	 * @return	List of Names and corresponding values
	 */
	public static Map<String, String> toNameValues(List<String> lines) {
		String[] division = null;
		Map<String, String> mapNameValue = 
			new HashMap<String, String>(lines.size());
		
		for (String line : lines) {
			division = StringUtils.getStrings(line, '=');
			mapNameValue.put(division[0],division[1]); 
		}
		return mapNameValue;
	}
	
	/**
	 * Parses the given file as a name value entity.
	 * This helps in loading a file directly for processing.
	 * TODO:// Move the loading process from the hadoop.
	 * @param fileName	Filename
	 * @return	Given a file name, extract name-value pairs
	 * @throws ApplicationFault
	 */
	public static Map<String, String> toNameValues(String fileName)
	throws ApplicationFault, SystemFault {
		
		File aFile = getFile(fileName);
		
		BufferedReader reader = null;
		InputStream stream = null;
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			Map<String, String> mapNameValue = new HashMap<String, String>(3);
			String line = null;
			String[] division = null;
			while((line=reader.readLine())!=null) {
				if (line.length() == 0) {
					continue;
				}
				char first=line.charAt(0);
				switch (first) {
					case ' ' : case '\n' : case '#' :  // skip blank & comment lines
					continue;
				}
				division = StringUtils.getStrings(line, '=');
				mapNameValue.put(division[0],division[1]); 
			}
			return mapNameValue;
		} catch (Exception ex) {
			throw new SystemFault(ex);
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {l.error("FileReaderUtil", ex);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {l.error("FileReaderUtil", ex);}
		}
		
	}
	
	/**
	 * Give in Strings
	 * @param fileName
	 * @return	All content of the file comes as a String object
	 * @throws ApplicationFault
	 */
	public static String toString(String fileName) 
	throws SystemFault, ApplicationFault {
		
		File aFile = getFile(fileName);
		BufferedReader reader = null;
		InputStream stream = null;
		StringBuilder sb = new StringBuilder();
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			String line = null;
			String newline = StringUtils.getLineSeaprator();
			while((line=reader.readLine())!=null) {
				if (line.length() == 0) continue;
				sb.append(line).append(newline);	
			}
			return sb.toString();
		} catch (Exception ex) {
			throw new SystemFault(ex);
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {l.error("FileReaderUtil", ex);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {l.error("FileReaderUtil", ex);}
		}
	}
	
	/**
	 * Returns the contents of the file in a byte array.
	 */
    public static byte[] getBytes(File file) throws ApplicationFault, SystemFault {
    	
    	InputStream is = null;
    	try {
    		is = new FileInputStream(file);
    	
	        long length = file.length();
	        if (length > Integer.MAX_VALUE) {
	        	throw new ApplicationFault(file.getAbsolutePath() + 
	        		" file is too long, " + length + " byte. Max allowed limit is " + Integer.MAX_VALUE);
	        }
    
	        byte[] bytes = new byte[(int)length]; // Create the byte array to hold the data
	        int offset = 0; // Read in the bytes
	        int numRead = 0;
	        
	        while (offset < bytes.length
	        	&& (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
	        	offset += numRead;
	        }
	        
	        if (offset < bytes.length) { // Ensure all the bytes have been read in
	            throw new SystemFault("Could not completely read file, " + file.getAbsolutePath());
	        }
	        
	        return bytes;
        } catch (IOException ex) {
        	l.fatal("util.FileReaderUtil > ", ex);
        	throw new SystemFault(ex);
        } finally {
        	try { if ( null != is ) is.close(); } 
        	catch (Exception ex) {l.warn("util.FileReaderUtil > ", ex);} 
        }        	
    }
	
	/**
	 * Resolves a file from various location..
	 * <lu>
	 * <li> Supplied location </li>
	 * <li> conf directory </li>
	 * <li> resources folder </li>
	 * <li> Class loader </li>
	 * </lu>
	 * @param fileName	Name of the file
	 * @return	File object
	 * @throws ApplicationFault
	 */
    public static File getFile(String fileName) throws ApplicationFault {
		File aFile = new File(fileName);
		if (aFile.exists()) return aFile;
		
		aFile = new File("/" + fileName);
		if (aFile.exists()) return aFile;

		aFile = new File("conf/" + fileName);
		if (aFile.exists()) return aFile;
		
		aFile = new File("resources/" + fileName);
		if (aFile.exists()) return aFile;
		
		try {
			URL resource = FileReaderUtil.class.getClassLoader().getResource(fileName);
			if ( resource != null) aFile = new File(resource.toURI());
		} 
		catch (URISyntaxException ex) {
			throw new ApplicationFault(ex);
		}

		if (aFile.exists()) return aFile;

		throw new ApplicationFault("FileResourceUtil > File does not exist :" + fileName);
	}
    
    public static String getHostNeutralFileName(String strUrl) {
    	if ( strUrl.indexOf(':') < 0) return  strUrl;
    	strUrl = strUrl.replace("D:", "/D:");
    	strUrl = strUrl.replace("C:", "/C:");
    	strUrl = strUrl.replace("d:", "/d:");
    	strUrl = strUrl.replace("c:", "/c:");
    	return strUrl;
    }
		
	
}
