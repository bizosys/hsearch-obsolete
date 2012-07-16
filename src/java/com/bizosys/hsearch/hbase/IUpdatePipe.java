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
 * Callback function during a update process. 
 * It helps in merging with already stored bytes in a locked environment. 
 * @author karan
 *
 */
public interface IUpdatePipe {

	/**
	 * Calls back after reading existing data
	 * @param family Existing Family
	 * @param name  Existing column Qualifier
	 * @param existingB Existing value
	 * @return	New column value as byte array
	 */
	byte[] process(byte[] family, byte[] name, byte[] existingB);
}
