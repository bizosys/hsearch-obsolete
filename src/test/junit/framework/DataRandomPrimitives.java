/*
* Copyright 2010 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
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
package junit.framework;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.bizosys.oneline.ApplicationFault;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.util.FileReaderUtil;

public class DataRandomPrimitives {
	
	public static long lastSeed = Long.MIN_VALUE;
	
	public static void main(String[] args) throws Exception  {
		PrintStream sysout = new PrintStream(System.out, true, "UTF-8");
		sysout.println(getShort(10));
	}
	
	/**
	 * Gets integer using random values
	 * @param total
	 * @return
	 */
	public static List<Integer> getInteger(int total) {
		Random random = new Random(System.currentTimeMillis() + lastSeed++);
		ArrayList<Integer> values = new ArrayList<Integer>(total);
		for ( int i=0; i< total; i++) {
			values.add(random.nextInt());
		}
		return values;
	}

	/**
	 * Gets long using random values
	 * @param total
	 * @return
	 */
	public static List<Long> getLong(int total) {
		Random random = new Random(System.currentTimeMillis()+ lastSeed++);
		ArrayList<Long > values = new ArrayList<Long>(total);
		for ( int i=0; i< total; i++) {
			values.add(new Long(random.nextLong()));
		}
		return values;
	}
	
	/**
	 * Gets double using random values
	 * @param total
	 * @return
	 */
	public static List<Double> getDouble(int total) {
		Random random = new Random(System.currentTimeMillis()+ lastSeed++);
		ArrayList<Double> values = new ArrayList<Double>(total);
		for ( int i=0; i< total; i++) {
			values.add(new Double(random.nextDouble()));
		}
		return values;
	}
	
	/**
	 * Gets floats using random values
	 * @param total
	 * @return
	 */
	public static List<Float> getFloat(int total) {
		Random random = new Random(System.currentTimeMillis()+ lastSeed++);
		ArrayList<Float> values = new ArrayList<Float>(total);
		for ( int i=0; i< total; i++) {
			values.add(new Float(random.nextFloat()));
		}
		return values;
	}
	
	/**
	 * Gets short using random values
	 * @param total
	 * @return
	 */
	public static List<Short> getShort(int total) {
		Random random = new Random(System.currentTimeMillis()+ lastSeed++);
		ArrayList<Short> values = new ArrayList<Short>(total);
		for ( int i=0; i< total; i++) {
			Integer intValue = random.nextInt(Short.MAX_VALUE);
			if ( intValue % 2 == 0 ) intValue = intValue * -1;
			values.add(intValue.shortValue());
		}
		return values;
	}
	
	public static List<Byte> getByte(int total) {
		Random random = new Random(System.currentTimeMillis()+ lastSeed++);
		ArrayList<Byte> values = new ArrayList<Byte>(total);
		for ( int i=0; i< total; i++) {
			Byte byteValue = (byte) random.nextInt(255 + Byte.MIN_VALUE);
			values.add(byteValue);
		}
		return values;
	}

	/**
	 * Gets short using random values
	 * @param total
	 * @return
	 */
	public static List<Boolean> getBoolean(int total) {
		Random random = new Random(System.currentTimeMillis()+ lastSeed++);
		ArrayList<Boolean> values = new ArrayList<Boolean>(total);
		for ( int i=0; i< total; i++) {
			values.add(random.nextBoolean());
		}
		return values;
	}
	
	/**
	 * Gets bytes using random values
	 * @param total
	 * @return
	 */
	public static List<byte[]> getBytes(int total) {
		List<String> strValues = getString(total);
		List<byte[]> byteValues = new ArrayList<byte[]>(total);
		for ( String strVal : strValues) {
			byteValues.add(strVal.getBytes());
		}
		return byteValues;
	}
	
	/**
	 * Gets Dates
	 * @param total
	 * @return
	 */	
	public static List<Date> getDates(int total) {
		long today = new Date().getTime();
		
		List<Integer> deltas = getInteger(total);
		List<Date> dtValues = new ArrayList<Date>(total);
		for ( Integer delta : deltas) {
			dtValues.add(new Date(today - delta));
		}
		return dtValues;
	}		

	
	/**
	 * Gets Strings random nouns
	 * @param total
	 * @return
	 */
	public static List<String> getString(int total) {
		File aFile = null;
		try {
			aFile = FileReaderUtil.getFile("nouns.txt");
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(1);
		}
		return pickRandomFromFile(total, aFile, 10000);
	}

	/**
	 * Gets Strings random nouns
	 * @param total
	 * @return
	 */
	public static List<String> getUnicodeString(int total) {
		File aFile = null;
		try {
			aFile = FileReaderUtil.getFile("hindi.txt");
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			return null;
		}
		return pickRandomFromFile(total, aFile, 20);
	}
	
	private static List<String> pickRandomFromFile(int total, File aFile, int maxSize) {
		BufferedReader reader = null;
		InputStream stream = null;
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			List<String> lines = new ArrayList<String>();
			String line = null;
			int counter = 0;
			
			while ( counter++ < total ) {
				
				int wordPos = 
					new Random(System.currentTimeMillis() + lastSeed++).nextInt(maxSize);
				int index = 0;
				while( ( (line=reader.readLine())!=null )) {
					if ( index++ < wordPos ) continue;
					if (line.length() == 0) continue;
					lines.add(line);
					break;
				}
			}
			return lines;
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			System.exit(1);
			return null;
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {System.err.println(ex);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {System.err.println(ex);}
		}
	}
	
	
	/**
	 * Gets bytes using random values
	 * @param total
	 * @return
	 */
	public static List<Storable> getStorable(int total) throws ApplicationFault {
		List<String> strValues = getString(total);
		List<Storable> byteValues = new ArrayList<Storable>(total);
		for ( String strVal : strValues) {
			byteValues.add(new Storable(strVal.getBytes()));
		}
		return byteValues;
	}	
}
