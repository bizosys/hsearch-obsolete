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

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;

/**
 * A name-value field which stores data in byte array.
 */
public class ByteField extends Storable {

	public static final ByteField[] EMPTY_BYTEFIELDS = new ByteField[]{};
	/**
	 * Field name
	 */
	public String name = null;
	
	/**
	 * Field name in bytes. Computed based on 
	 * supplied name during comparision and storage
	 */
	public byte[] nameBytes = null;
	
	/**
	 * What is the type field data type.
	 * Use Storable.BYTE_STRING, ...
	 */
	public boolean isTypeOnToBytes = false;
	
	/**
	 * Constructor : Keeps the value as bytes
	 * @param name : Field name
	 * @param origVal	Value in bytes.
	 */
	public ByteField(String name, byte[] origVal) {
		super(origVal);
		this.name = name;
	}

	/**
	 * Parses the field byte values to the given data type.
	 * @param name :  Field name
	 * @param inputBytes : Value in bytes
	 * @param type : Use Storable.BYTE_STRING ..
	 */
	public ByteField(String name, byte[] inputBytes, byte type) {
		super(inputBytes, type);
		this.name = name;
	}
	
	/**
	 * The field reading from a given start position to the required 
	 * number of bytes (long, short, int,..) or till the end(String)
	 * @param name	Field Name
	 * @param inputBytes	Bytes
	 * @param type	Field Type (see <code>Storable</code>
	 * @param startPos	The bytes array reading starting position
	 */
	public ByteField(String name, byte[] inputBytes, byte type, int startPos) {
		super(inputBytes, type,startPos);
		this.name = name;
	}

	/**
	/**
	 * The field reading from a given start position till 
	 * the specified end position
	 * @param name	Field Name
	 * @param inputBytes	Input Byte array
	 * @param type	Field Type (see <code>Storable</code>
	 * @param startPos	The bytes array reading starting position
	 * @param endPos	The bytes array reading end position
	 */
	public ByteField(String name, byte[] inputBytes, byte type, 
	int startPos, int endPos ) {
		
		super(inputBytes, type,startPos,endPos);
		this.name = name;
	}
	
	/**
	 * Any serializable object which implements IStorable interface
	 * @param name	Field Name
	 * @param storable	Value Object
	 */
	public ByteField(String name, IStorable storable) {
		super(storable);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	String Value
	 */
	public ByteField(String name, String origVal) {
		super(origVal);
		this.name = name;
	}

	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Byte  Value
	 */
	public ByteField(String name, Byte origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Short Value
	 */
	public ByteField(String name, Short origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Integer Value
	 */
	public ByteField(String name, Integer origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Long Value
	 */
	public ByteField(String name, Long origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Float Value
	 */
	public ByteField(String name, Float origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Double Value
	 */
	public ByteField(String name, Double origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Boolean Value
	 */
	public ByteField(String name, Boolean origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Character Value
	 */
	public ByteField(String name, Character origVal) {
		super(origVal);
		this.name = name;
	}

	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Date Value
	 */
	public ByteField(String name, java.util.Date origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param name	Field Name
	 * @param origVal	Date Value
	 */
	public ByteField(String name, java.sql.Date origVal) {
		super(origVal);
		this.name = name;
	}
	
	/**
	 * Set the Field Name
	 * @param name	Field name
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Get the field name
	 * @return	Name as byte field
	 */
	public byte[] getName() {
		if (null == nameBytes) nameBytes = Storable.putString(name);
		return nameBytes;
	}
	
	/**
	 * Specify whether the first bit is type
	 * @param status	Carry the data type as first bit
	 */
	public void enableTypeOnToBytes(boolean status) {
		this.isTypeOnToBytes = status;
	}
	
	@Override
	public byte[] toBytes() {
		if ( this.isTypeOnToBytes ) {
			int byteT = this.byteValue.length; 
			byte[] bytesWithType = new byte[byteT + 1]; 
			System.arraycopy(this.byteValue, 0, bytesWithType, 0, byteT);
			bytesWithType[byteT] = this.type;
			return bytesWithType;
		} else {
			return this.byteValue;
		}
	}

	/**
	 * Wrap a byte array as ByteField
	 * @param name	The field name
	 * @param inputBytes	the input byte
	 * @return	Constructed byte field
	 */
	public static ByteField wrap( byte[] name, byte[] inputBytes) {
		
		if ( null == inputBytes ) return null;
		int inputBytesT = inputBytes.length;
		if ( inputBytesT < 1) return null;
		
		byte typeIdentifier = inputBytes[inputBytesT - 1];
		return new ByteField( new String(name),
			inputBytes, typeIdentifier, 0, inputBytesT - 1);
	}
	
	@Override
	public String toString() {
		return "ByteField Name:" + this.name + " , Value = " + this.asIsObject;
	}
}