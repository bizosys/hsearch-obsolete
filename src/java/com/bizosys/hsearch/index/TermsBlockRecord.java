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
package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.util.Record;

/**
 * This is a Row from Index table which has the 
 * ability to merge and store with previous values.
 * @author karan
 *
 */
public class TermsBlockRecord extends Record {
	
	TermFamilies termFamilies;
	
	public TermsBlockRecord(IStorable pk) {
		super(pk);
	}
	
	public TermsBlockRecord(IStorable pk, List<NV> kvs ) {
		super(pk, kvs);
	}
	
	public void setTermFamilies(TermFamilies termFamilies) {
		this.termFamilies = termFamilies;
	}
	
	@Override
	public List<NV> getBlankNVs() throws IOException {
		return getNVs();
	}	
	
	@Override
	public List<NV> getNVs() {
		List<NV> nvs = new Vector<NV>(200);
		termFamilies.toNVs(nvs);
		return nvs;
	}	
	
	@Override
	public boolean merge(byte[] fam, byte[] name, byte[] existingB) {
		char family = (char) fam[0];
		char col = (char) name[0];
		return merge(family,col,existingB);
	}

	public boolean merge(char family, char col, byte[] existingB) {
		
		if ( ! termFamilies.families.containsKey(family)) {
			TermList terms = new TermList();
			terms.setExistingBytes(existingB);
			TermColumns tcs = new TermColumns(family);
			tcs.add(col, terms);
			termFamilies.families.put(family, tcs);
			return true;
		}
		
		TermColumns cols = termFamilies.families.get(family);
		if ( ! cols.columns.containsKey(col)) {
			TermList terms = new TermList();
			terms.setExistingBytes(existingB);
			cols.add(col, terms);
			return true;
		}
		TermList terms = cols.columns.get(col);
		terms.setExistingBytes(existingB);
		return true;
	}
	
	public void cleanup() {
		if ( null == this.termFamilies) return;
		this.termFamilies.cleanup();
	}
}
