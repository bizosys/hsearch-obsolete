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
package com.bizosys.hsearch.util;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.NV;

/**
 * A Record 
 * @author bizosys
 *
 */
public class RecordScalar {

	public IStorable pk = null;
	public NV kv = null;
	public long checkoutTime = -1L; 

	public RecordScalar(IStorable pk) {
		this.pk = pk;
	}

	public RecordScalar(IStorable pk, NV kv ) {
		this.pk = pk;
		this.kv = kv;
	}
	
	public RecordScalar(byte[] pk, NV kv ) {
		this.pk = new Storable(pk);
		this.kv = kv;
	}
	
	public String toString() {
		return ("Record = " + this.pk);
	}

	public boolean merge(byte[] existingB) {
		return true;
	}

}
