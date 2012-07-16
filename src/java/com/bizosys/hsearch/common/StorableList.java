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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;

/**
 * A serializable list. The list can hold and IStorable data types
 * @see Storable 
 */
public class StorableList implements List, IStorable {

	/**
	 * Container of Array of bytes
	 */
	List<byte[]> container = null;
	
	/**
	 * Default constructor
	 */
	public StorableList() {
		container = new ArrayList<byte[]>();
	}
	
	/**
	 * Initialize with the size
	 * @param size
	 */
	public StorableList(int size) {
		container =  new ArrayList<byte[]>(size);
	}
	
	/**
	 * Initialize with the stored serialized bytes
	 * @param inputBytes
	 */
	public StorableList(byte[] inputBytes) {
		this(inputBytes, 0, inputBytes.length);
	}
	
	/**
	 * Initialize by deserializing the stored serialized bytes 
	 * @param inputBytes	Input bytes
	 * @param offset	Read start position
	 * @param length	Number of bytes to read
	 */
	public StorableList(byte[] inputBytes, int offset, int length) {
		init(inputBytes, offset, length);
	}

	
	/**
	 * Add an IStorable object
	 */
	public boolean add(Object storable) {
		byte[] bytes = ((IStorable)storable).toBytes(); 
		container.add(bytes); 
		return true;
	}

	public void add(int pos, Object storable) {
		byte[] bytes = ((IStorable)storable).toBytes(); 
		container.add(pos, bytes); 
	}

	public boolean addAll(Collection colStorable) {
		for (Object storable : colStorable) {
			byte[] bytes = ((IStorable)storable).toBytes(); 
			container.add(bytes); 
		}
		return true;
	}

	public void clear() {
		this.container.clear();
	}

	public Object get(int arg0) {
		return this.container.get(arg0);
	}

	public boolean addAll(int arg0, Collection arg1) {
		return false;
	}

	public boolean contains(Object arg0) {
		return false;
	}

	public boolean containsAll(Collection arg0) {
		return false;
	}

	public int indexOf(Object arg0) {
		return 0;
	}

	public boolean isEmpty() {
		return this.container.isEmpty();
	}

	public Iterator iterator() {
		return this.container.iterator();
	}

	public int lastIndexOf(Object arg0) {
		return 0;
	}

	public ListIterator listIterator() {
		return null;
	}

	public ListIterator listIterator(int arg0) {
		return null;
	}

	public boolean remove(Object arg0) {
		return false;
	}

	public Object remove(int arg0) {
		container.remove(arg0); 
		return null;
	}

	public boolean removeAll(Collection arg0) {
		return false;
	}

	public boolean retainAll(Collection arg0) {
		return false;
	}

	public Object set(int arg0, Object arg1) {
		return null;
	}

	public int size() {
		return this.container.size();
	}

	public List subList(int arg0, int arg1) {
		return null;
	}

	public Object[] toArray() {
		return null;
	}

	@SuppressWarnings("unchecked")
	public Object[] toArray(Object[] arg0) {
		return null;
	}
	
	public byte[] toBytes() {
		byte[] outputBytes = null;
		
		if ( null != container) {
			int totalBytes = 0;
			for (byte[] bytes : container) {
				totalBytes = totalBytes + 2 + bytes.length ; //2 is added as size
			}
			
			outputBytes = new byte[totalBytes];
			int seek = 0;
			for (byte[] bytes : container) {

				short byteSize = (short) bytes.length;
				outputBytes[seek++] = (byte)(byteSize >> 8 & 0xff); 
				outputBytes[seek++] = (byte)(byteSize & 0xff) ;
				System.arraycopy(bytes, 0, outputBytes, seek, byteSize);
				seek = seek + byteSize;
			}
		}
		return outputBytes;
	}
	
	public int fromBytes(byte[] data, int pos) {
		if ( null == data) return pos;
		return init(data, pos, data.length - pos);
	}
	
	private int init(byte[] inputBytes, int offset, int length) {
		if ( null == inputBytes) return offset;
		this.container = new ArrayList<byte[]>();
		int curPos = offset;
		int endPos = offset + length;
		
		while ( curPos < endPos ) {
			int contentSize = 0;  //The content size is in short
			contentSize = (inputBytes[curPos++] << 8 ) + ( inputBytes[curPos++] & 0xff );
			byte[] contentBytes = new byte[contentSize];
			System.arraycopy(inputBytes, curPos, contentBytes, 0, contentSize);
			curPos = curPos + contentSize;
			container.add(contentBytes);
		}
		return endPos;
	}	
		
}