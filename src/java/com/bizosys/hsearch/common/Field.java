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

import com.bizosys.oneline.SystemFault;

/**
 * A field is a section of a Document Content.
 * Each field has two parts, a name and a value. 
 * Values may be free text, provided as a String or as any Java data type 
 * or serializable atomic object implementing IStorable interface. 
 */
public interface Field {
	/**
	 * Specifies whether a field should be indexed.
	 * @return	True is Indexable
	 */
	boolean isIndexable();
	
	/**
	 * Specifies whether a field should be analyzed for extracting words.
	 * @return	True if requires analysis
	 */
	boolean isAnalyze();
	
	/**
	 * Specifies whether a field should be stored.
	 * @return	True if storing
	 */
	boolean isStore();
	
	/**
	 * Get the name and value of the field
	 * @return	Returns the <code>ByteField</code>
	 * @throws SystemFault	Any issue on parsing throws SystemFault
	 */
	ByteField getByteField() throws SystemFault;
}