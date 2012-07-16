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

import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.Record;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * Multiple term families grouped inside a Termtable
 * @author karan
 *
 */
public class TermTables {
	
	private static final int NV_FLUSH_LIMIT = 256;
	static {Account.init();}
	
	public IStorable bucketId = null;
	public Map<Character, TermFamilies> tables = null;
	private boolean threadSafe = true;
	
	public TermTables(boolean concurrency) {
		this.threadSafe = concurrency;
	}
	
	public TermTables(IStorable bucketId, boolean concurrency) {
		this.bucketId = bucketId;
		this.threadSafe = concurrency;
	}
	
	public void add(Term aTerm, ILanguageMap lang) {
		if ( null == tables) tables = new HashMap<Character, TermFamilies>();
		if (StringUtils.isEmpty(aTerm.term) ) return;
		Character table = lang.getTableName(aTerm.term);
		TermFamilies block = null;
		if ( tables.containsKey(table)) block  = tables.get(table);
		else {
			block = new TermFamilies();
			tables.put(table, block);
		}
		block.add(aTerm, lang);
	}
	
	public boolean add(TermTables another) {
		if ( null == another.bucketId) return false;
		
		byte[] anotherPK = another.bucketId.toBytes();
		if ( !Storable.compareBytes(this.bucketId.toBytes(), anotherPK) ) return false;
		
		/**
		 * Both belong to same bucket zone
		 */
		for (Character otherTable : another.tables.keySet()) {
			TermFamilies otherFamilies = another.tables.get(otherTable);
			
			if (this.tables.containsKey(otherTable)) {
				TermFamilies thisFamilies = this.tables.get(otherTable);
				thisFamilies.add(otherFamilies);
			} else {
				this.tables.put(otherTable, otherFamilies);
			}
		}
		return true;
	}

	/**
	 * Both belong to same bucket zone
	 * @param another
	 * @return
	 */
	public void addInSameBucket(TermTables another) {
		for (Character otherTable : another.tables.keySet()) {
			TermFamilies otherFamilies = another.tables.get(otherTable);
			if (this.tables.containsKey(otherTable)) {
				TermFamilies thisFamilies = this.tables.get(otherTable);
				thisFamilies.add(otherFamilies);
			} else {
				this.tables.put(otherTable, otherFamilies);
			}
		}
	}

	public void assignDocumentPosition(int docPos) {
		if ( null == tables) return;
		for ( TermFamilies tf : tables.values()) {
			if ( null == tf ) continue;
			tf.assignDocumentPosition(docPos);
		}
	}
	
	public void persist(boolean merge, boolean newBucket) throws SystemFault {
		TermsBlockRecord tbr = null;
		try {
			for ( Character tableName : tables.keySet()) {
				if ( newBucket ) {
					List<NV> nvs = ObjectFactory.getInstance().getNVList();
					TermFamilies termFamilies = tables.get(tableName);

					int nvT = 0;
					for ( char fam: termFamilies.families.keySet() ) {
						TermColumns tc = termFamilies.families.get(fam);
						tc.toNVs(nvs);
						nvT = nvs.size();
						if ( nvT > NV_FLUSH_LIMIT) {
							if ( IndexLog.l.isInfoEnabled() ) IndexLog.l.info("Total records size :" + nvT);
							Record record = new Record(bucketId,nvs);
							HWriter.getInstance(threadSafe).insert(tableName.toString(), record);
							nvs.clear();
						}
					}
					
					if ( nvs.size() > 0) {
						Record record = new Record(bucketId,nvs);
						HWriter.getInstance(threadSafe).insert(tableName.toString(), record);
						nvs.clear();
					}
					
					ObjectFactory.getInstance().putNVList(nvs);
					
				} else {
					TermFamilies termFamilies = tables.get(tableName);
					tbr = new TermsBlockRecord(bucketId);
					tbr.setTermFamilies(termFamilies); 
					if  (HbaseLog.l.isDebugEnabled()) 
						HbaseLog.l.debug("TermTables.persist Table " + tableName + tbr.toString());
					HWriter.getInstance(threadSafe).merge(tableName.toString(), tbr);
					tbr.cleanup();
				}
			}
		} catch (Exception ex) {
			if ( null != tbr) tbr.cleanup();
			throw new SystemFault(ex);
		}
	}
	
	/**
	 * Populates the existing value.
	 * @param tableName
	 * @param termFamilies
	 * @throws SystemFault
	 */
	public void setExistingValue(String tableName, 
		TermFamilies termFamilies) throws SystemFault {
		
		List<NVBytes> existingB = 
			HReader.getCompleteRow(tableName, bucketId.toBytes());
		if ( null == existingB) return;
		
		for (char family: termFamilies.families.keySet()) {
			TermColumns cols = termFamilies.families.get(family);
			for (char col : cols.columns.keySet()) {
				TermList terms = cols.columns.get(col);
				
				for (NVBytes bytes : existingB) {
					if ( bytes.family[0] == family && bytes.name[0] == col) {
						terms.setExistingBytes(bytes.data);
						break;
					}
				}
			}
		}
	}
	
	/**
	 * Calculates the total term inside the container
	 * @return	Total number of terms present
	 */
	public int getTableSize() {
		if ( null == this.tables) return 0;
		int totalsize = 0;
		for (TermFamilies tf : this.tables.values()) {
			if ( null == tf.families) continue;
			for ( TermColumns tcs : tf.families.values()) {
				if ( null == tcs.columns) continue;
				for (TermList tl : tcs.columns.values()) {
					if ( null == tl.lstKeywords) continue;
					for ( List<Term> terms: tl.lstKeywords.values()) {
						totalsize = totalsize + terms.size();
					}
				}
			}
		}
		return totalsize;
	}
	
	public void cleanup() {
		if ( null == tables) return;
		for (TermFamilies families: tables.values()) {
			families.cleanup();
		}
		tables.clear();
	}
}