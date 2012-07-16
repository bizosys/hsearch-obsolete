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

/**
 * Term Columns keep multiple terms belonging to the column 
 * containing that term.
 * @author karan
 *
 */
public class TermColumns implements IDimension {
	
	/**
	 * Each column containing the column and the list
	 * Map<keyword,Termlist>
	 */
	public char family;
	public Map<Character, TermList> columns = null;
	
	public TermColumns(char columnFamily) {
		this.family = columnFamily;
	}
	
	/**
	 * Add a keyword. Repetition are taken care
	 * @param aTerm
	 */
	public void add(Character col, Term aTerm) {
		if ( null == aTerm) return;

		if ( null == columns) {
			columns = new HashMap<Character, TermList> ();
			TermList tl = new TermList();
			tl.add(aTerm);
			columns.put(col, tl);
			return;
		}
		
		if ( columns.containsKey(col)) {
			columns.get(col).add(aTerm);
		} else {
			TermList tl = new TermList();
			tl.add(aTerm);
			columns.put(col, tl);
		}
	}
	
	public void add(TermColumns otherCols) {
		if ( null == otherCols) return;
		if ( null == otherCols.columns) return;
		for (char col: otherCols.columns.keySet()) {
			TermList otherTerms = otherCols.columns.get(col);
			if ( this.columns.containsKey(col)) {
				this.columns.get(col).add(otherTerms);
			} else {
				this.columns.put(col, otherTerms);
			}
		}
	}
	
	/**
	 * This initializes the existing term column with the provided termlist
	 * @param tl
	 */
	public void add(Character col, TermList tl) {
		if ( null == tl) return;

		if ( null == columns) {
			columns = new HashMap<Character, TermList> ();
		}
		
		columns.put(col, tl);
		return;
	}
	

	
	/**
	 * The given document id will be applied to 
	 * @param position
	 */
	public void assignDocPos(int position) {
		if ( null == this.columns) return;
		short pos = (short) position;
		for (TermList termL : this.columns.values()) {
			termL.assignDocPos(pos);
		}
	}
	
	/**
	 * Serialize this
	 */
	public void toNVs(List<NV> nvs) {
		if ( null == columns) return;
		String strFamily = new String(new char[]{this.family});
		for (char col: columns.keySet()) {
			String strCol = new String(new char[]{col});
			TermList tl = columns.get(col);
			nvs.add(new NV(strFamily, strCol, 
				columns.get(col),tl.isExistingUnchanged()));
		}
	}
	
	public void cleanup() {
		if ( null == columns) return;
		for (TermList tl : columns.values()) {
			tl.cleanup();
		}
		columns.clear();
		columns = null;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nTermColumn: ");
		if ( null == this.columns) {
			sb.append("None");
			return sb.toString();
		}
		
		for ( char col: this.columns.keySet()) {
			sb.append("\nHash ").append(col).append(' ');
			sb.append(this.columns.get(col).toString());
		}
		return sb.toString();
	}

}
