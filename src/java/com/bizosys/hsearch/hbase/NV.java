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

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;

/**
 * Serialized name value objects
 * @author karan
 *
 */
public class NV {

	/**
	 * Column Family
	 */
	public IStorable family = null;
	
	/**
	 * Column name
	 */
	public IStorable name = null;
	
	/**
	 * Column data
	 */
	public IStorable data = null;
	
	public boolean isDataUnchanged = false;

	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 */
	public NV(String family, String name) {
		this.family = new Storable(family);
		this.name = new Storable(name);
	}
	
	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 */
	public NV(byte[] family, byte[] name) {
		this.family = new Storable(family);
		this.name = new Storable(name);
	}
	
	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 * @param data	Column Value
	 */
	public NV(byte[] family, byte[] name, IStorable data) {
		this.family = new Storable(family);
		this.name = new Storable(name);
		this.data = data;
	}
	
	public NV(byte[] family, byte[] name, IStorable data, boolean isDataUnchanged) {
		this(family,name,data);
		this.isDataUnchanged = isDataUnchanged;
	}
	
	
	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 * @param data	Column Value
	 */
	public NV(String family, String name, IStorable data ) {
		this.family = new Storable(family);
		this.name = new Storable(name);
		this.data = data;
	}
	
	public NV(String family, String name, IStorable data, boolean isDataUnChanged ) {
		this.family = new Storable(family);
		this.name = new Storable(name);
		this.data = data;
		this.isDataUnchanged = isDataUnChanged;
	}
	

	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 * @param data	Column Value
	 */
	public NV(IStorable family, IStorable name, IStorable data ) {
		this.family = family;
		this.name = name;
		this.data = data;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(50);
		sb.append("  F:[").append(new String(family.toBytes())).
		append("] N:[").append(new String(name.toBytes())).
		append("] D:").append(data.toString());
		return sb.toString();
	}
}
