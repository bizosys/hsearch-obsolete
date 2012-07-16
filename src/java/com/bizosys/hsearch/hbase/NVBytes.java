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
package com.bizosys.hsearch.hbase;

/**
 * bytes name value object. This helps working with raw byte arrays
 * and hence avoiding any intermediate objects and the data conversion process.
 * @author karan
 *
 */
public class NVBytes {

	/**
	 * Column Family
	 */
	public byte[] family = null;

	/**
	 * Column name
	 */
	public byte[] name = null;

	/**
	 * Column data
	 */
	public byte[] data = null;

	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 */
	public NVBytes(byte[] family, byte[] name) {
		this.family = family;
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 * @param data	Column Value
	 */
	public NVBytes(byte[] family, byte[] name, byte[] data) {
		this.family = family;
		this.name = name;
		this.data = data;
	}

	@Override
	public String toString() {
		return new String(family) + ":" + new String(name) +  ":" + new String(data);
	}
}
