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
package com.bizosys.hsearch.inpipe;

import java.util.List;

import com.bizosys.hsearch.hbase.IUpdatePipe;
import com.bizosys.hsearch.index.InvertedIndex;

/**
 * In a merged index, delete only relevant document keywords
 * @author karan
 *
 */
public class DeleteFromIndexWithCut implements IUpdatePipe {
	private static final boolean DEBUG_ENABLED = InpipeLog.l.isDebugEnabled();
	List<Short> docSerialIds = null;
	public DeleteFromIndexWithCut(List<Short> docSerialIds) {
		this.docSerialIds = docSerialIds; 
	}
	
	public byte[] process(byte[] family, byte[] name, byte[] existingB) {
		byte[] modifiedByte = existingB;
		for (short docSerialId : docSerialIds) {
			modifiedByte = InvertedIndex.delete(modifiedByte, docSerialId);
		}

		int existingBT = ( null == existingB) ? 0 : existingB.length;
		int modifiedByteT = ( null == modifiedByte) ? 0 : modifiedByte.length;
		if ( DEBUG_ENABLED) {
			InpipeLog.l.debug("At pipe:" + new String(family) + "/" +
				new String(name) + "/" + existingBT + "/" + modifiedByteT);
		}
		
		if ( null == modifiedByte) return null; 
		return modifiedByte;
	}

}
