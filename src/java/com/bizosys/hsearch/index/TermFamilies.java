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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.ILanguageMap;

/**
 * This contain multiple term columns for a given term family.
 * @author karan
 *
 */
public class TermFamilies implements IDimension {
	
	/**
	 * Family, Columns
	 */
	public Map<Character, TermColumns> families = null;
	
	/**
	 * Add a Term
	 * @param aTerm
	 * @param lang
	 */
	public void add(Term aTerm, ILanguageMap lang){
		
		char family = lang.getColumnFamily(aTerm.term);
		char column = lang.getColumn(aTerm.term);
		if ( IndexLog.l.isDebugEnabled() ) IndexLog.l.debug("TermFamilies.Add :" +
				aTerm.term + " Family:" + family + " column:" + column );
		
		if ( null == families) {
			families = new HashMap<Character, TermColumns>(ILanguageMap.ALL_FAMS.length);
		}
		
		TermColumns termCol = null;
		
		/**
		 * Create a term column
		 */
		if ( families.containsKey(family)) {
			termCol = families.get(family);
		} else {
			termCol = new TermColumns(family);
			families.put(family, termCol);
		}
		
		/** Add the term */
		termCol.add(column,aTerm);
	}
	
	/**
	 * Add another term family
	 * @param otherFamilies
	 */
	public void add(TermFamilies otherFamilies) {
		for ( Character otherFamily: otherFamilies.families.keySet()) {
			TermColumns otherCols = otherFamilies.families.get(otherFamily);
			if ( this.families.containsKey(otherFamily)) {
				TermColumns thisCols = this.families.get(otherFamily);
				thisCols.add(otherCols);
			} else {
				this.families.put(otherFamily, otherCols);
			}
		}
	}	

	public void toNVs(List<NV> nvs) {
		for ( char fam: families.keySet() ) {
			TermColumns tc = families.get(fam);
			tc.toNVs(nvs);
		}
	}
	
	public void assignDocumentPosition(int docPos) {
		if ( null == families ) return;
		for ( TermColumns columns : families.values()) {
			if ( null == columns) continue;
			columns.assignDocPos(docPos);
		}
	}

	public void cleanup() {
		if ( null == families) return;
		for (TermColumns tc: families.values()) {
			tc.cleanup();
		}
		families.clear();
		families = null;
	}
}
