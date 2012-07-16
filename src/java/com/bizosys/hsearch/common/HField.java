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
package com.bizosys.hsearch.common;

import java.util.Date;

import com.bizosys.hsearch.filter.IStorable;

/**
 * The field can carry any value
 * Java basic data types and other List, Complex Objects which has 
 * implemented <code>IStorable</code> interface for extracting the byte array.
 * @see IStorable
 */
public class HField implements Field {
	
	/**
	 *	Indexable field 
	 */
	private boolean isIndexable = true;
	
	/**
	 * Is analyzable.
	 */
	private boolean isAnalyzed = true;
	
	/**
	 * Requires storing
	 */
	private boolean isStored = true;
	
	/**
	 * The Name-Value
	 */
	private ByteField bfl = null;
	
	/**
	 * Constructor. Set Name-Value later. 
	 * @param isIndexable	Should Index
	 * @param isAnalyzed	Should Analyze
	 * @param isStored	Shoud Store
	 */
	public HField(boolean isIndexable,
		boolean isAnalyzed,boolean isStored ) {
		
		this.isIndexable = isIndexable;
		this.isAnalyzed = isAnalyzed;
		this.isStored = isStored;
	}
	
	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, String value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, long value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}
	
	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, byte value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, boolean value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, short value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}
	
	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, int value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, float value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, double value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, Date value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Set field Name and Value turning on indexing, analysis and storage.
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public HField(String name, byte[] value) {
		this.bfl = new ByteField(name, value);
		bfl.enableTypeOnToBytes(true);
	}

	/**
	 * Get the ByteField
	 */
	public ByteField getByteField() {
		return this.bfl;
	}

	/**
	 * Specifies whether a field should be analyzed for extracting words.
	 * @return	True if requires analysis
	 */
	public boolean isAnalyze() {
		return this.isAnalyzed;
	}

	/**
	 * Specifies whether a field should be indexed.
	 * @return	True is Indexable
	 */
	public boolean isIndexable() {
		return this.isIndexable;
	}

	/**
	 * Specifies whether a field should be stored.
	 * @return	True if storing
	 */
	public boolean isStore() {
		return this.isStored;
	}
}
