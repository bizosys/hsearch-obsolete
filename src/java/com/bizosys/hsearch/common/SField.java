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

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.oneline.SystemFault;

/**
 * Serializable Field for document content.
 * If using embedded in embedded mode use <code>HField</code> class.
 * @see HField
 */
public class SField implements Field{

	/**
	 *	Indexable field 
	 */
	private boolean index = true;
	
	/**
	 * Is analyzable.
	 */
	private boolean analyze = true;
	
	/**
	 * Requires storing
	 */
	private boolean store = true;
	
	/**
	 * Field type. @See <code>Storable</code> for allowed types
	 */
	public byte type = Storable.BYTE_STRING;
	
	/**
	 * Field name
	 */
	public String name;
	
	/**
	 * Field Value
	 */
	public String value;
	
	
	private ByteField bfl = null;
	
	public SField(String name, String value) {
			
			this.index = true;
			this.analyze = true;
			this.store = true;
			this.type = Storable.BYTE_STRING;
			this.name = name;
			this.value = value;
	}	
	
	/**
	 * Default Constructor
	 * @param index	Is Indexable
	 * @param analyze	Is Anlyzed
	 * @param store	Should Store
	 * @param type	Data Type @See <code>Storable</code> for allowed types
	 * @param name	Field Name
	 * @param value	Field Value
	 */
	public SField(boolean index,boolean analyze,boolean store,
		byte type, String name, String value) {
		
		this.index = index;
		this.analyze = analyze;
		this.store = store;
		this.type = type;
		this.name = name;
		this.value = value;
	}
	
	/**
	 * @return ByteField	The ByteField representation of name-value 
	 */
	public ByteField getByteField() throws SystemFault {
		if ( null != bfl) return bfl;
		if ( null != name) name = name.toLowerCase();
		switch (type) {
			case Storable.BYTE_BOOLEAN:
				bfl = new ByteField(name,new Boolean(value));
				break;
			case Storable.BYTE_BYTE:
				bfl = new ByteField(name,new Byte(value));
				break;
			case Storable.BYTE_CHAR:
				bfl = new ByteField(name,value.charAt(0));
				break;
			case Storable.BYTE_DATE:
				bfl = new ByteField(name,new Date(new Long(value)));
				break;
			case Storable.BYTE_DOUBLE:
				bfl = new ByteField(name,new Double(value));
				break;
			case Storable.BYTE_FLOAT:
				bfl = new ByteField(name,new Float(value));
				break;
			case Storable.BYTE_INT:
				bfl = new ByteField(name,new Integer(value));
				break;
			case Storable.BYTE_LONG:
				bfl = new ByteField(name,new Long(value));
				break;
			case Storable.BYTE_SHORT:
				bfl = new ByteField(name,new Short(value));
				break;
			case Storable.BYTE_STRING:
				bfl = new ByteField(name,value);
				break;
			default:
				throw new SystemFault("Unknown data type :" + type);
		}
		return bfl;
	}

	/**
	 * Specifies whether a field should be analyzed for extracting words.
	 * @return	True if requires analysis
	 */
	public boolean isAnalyze() {
		return this.analyze;
	}

	/**
	 * Specifies whether a field should be indexed.
	 * @return	True is Indexable
	 */
	public boolean isIndexable() {
		return this.index;
	}

	/**
	 * Specifies whether a field should be stored.
	 * @return	True if storing
	 */
	public boolean isStore() {
		return this.store;
	}
}
