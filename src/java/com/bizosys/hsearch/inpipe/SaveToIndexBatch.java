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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IndexLog;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermColumns;
import com.bizosys.hsearch.index.TermFamilies;
import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.index.TermTables;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.schema.SchemaManager;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.Record;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
 
/**
 * Saves the term vector to the index table in a batch mode. In the batch mode, it expects
 * 1) Bunch comes under one bucket only  
 * 2) Each bucket will be overriden with no merging
 * 3) At one time one client only operates on a bucket. So thread safety is not provided. 
 * @author karan
 *
 */
public class SaveToIndexBatch implements PipeIn {

	int docMergeFactor = 1000;
	/** Arranged by document */
	Map<Long, TermTables> mergedTermTables = null; 
	Map<Long, Map<String,byte[]>> mergedBytes = 
		new HashMap<Long, Map<String,byte[]>>(2) ;
	int bufferSize = 0;
	boolean isIdMappingEnabled = true;  
	
	public SaveToIndexBatch() {
		
	}
	
	public SaveToIndexBatch(int docMergeFactor, boolean isIdMappingEnabled) {
		this.docMergeFactor = docMergeFactor;
		this.isIdMappingEnabled = isIdMappingEnabled;
	}

	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;

		if ( null == doc.bucketId || null == doc.docSerialId ) 
			throw new ApplicationFault ("Ids missed from document :" + doc.toString());

		if ( null == doc.terms) return;
		if ( null == doc.terms.all) return;
		
		ILanguageMap map = SchemaManager.getInstance().getLanguageMap(doc.meta.locale);
		TermTables termTable = ( null == doc.bucketId ) ?
			new TermTables(multiWriter) : new TermTables( new Storable(doc.bucketId),multiWriter);
		
		short docId = doc.docSerialId;
		for (Term term : doc.terms.getTermList()) {
			term.setDocumentPosition(docId);
			termTable.add(term, map);
		}
		bufferSize = bufferSize + doc.terms.getTermList().size();
		doc.terms.closeTermList();

		/**
		 * We need to arrange all terms from documents to arrange in term buckets.
		 */
		if ( null == mergedTermTables)
			mergedTermTables = ObjectFactory.getInstance().getBucketTermTable();
		
		buildTermTables(doc.bucketId, mergedTermTables, termTable);
		
		/**
		 * ************** Check For Intermediate Flushing **************
		 */
		if ( bufferSize > 1000000) {
			if ( IndexLog.l.isInfoEnabled() ) 			 
				IndexLog.l.info("Flush Intermediate Size > " + bufferSize );
			flushIntermediate(this.mergedBytes, mergedTermTables);
		}
		/**
		 * ************** ----------------------- **************
		 */
	}

	/**
	 * Persist the Index
	 */
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == mergedTermTables) return;
		if ( IndexLog.l.isDebugEnabled()) printMergedTT(mergedTermTables);
		
		if ( mergedTermTables.size() > 0) {
			flushIntermediate(this.mergedBytes, mergedTermTables);
			this.mergedTermTables.clear();
		}

		if ( null != mergedTermTables) {
			ObjectFactory.getInstance().putBucketTermTable(mergedTermTables);
		}
		
		if ( this.mergedBytes.size() == 0 ) return;
		
		for (long bucketId : this.mergedBytes.keySet()) {
			if ( IndexLog.l.isInfoEnabled() ) 			 
				IndexLog.l.info("Commit > " + bucketId);
			Map<String,byte[]> values = this.mergedBytes.get(bucketId);
			Map<Character, List<NV>> tableNVs = new HashMap<Character, List<NV>>(); 
			for (String tfc : values.keySet()) {
				char tab = tfc.charAt(0);
				if (tableNVs.containsKey(tab) ) {
					List<NV> tabNvs = tableNVs.get(tab);
					tabNvs.add(new NV( new byte[]{(byte)tfc.charAt(1)}, 
						new byte[]{(byte)tfc.charAt(2)}, 
						new Storable(values.get(tfc))));
				} else {
					List<NV> nvs = ObjectFactory.getInstance().getNVList();					
					nvs.add(new NV( new byte[]{(byte)tfc.charAt(1)}, 
							new byte[]{(byte)tfc.charAt(2)}, 
							new Storable(values.get(tfc))));
					tableNVs.put(tab, nvs);
				}
			}
			
			values.clear();
			try {
				for (Character tab : tableNVs.keySet()) {
					List<NV> tabNV = tableNVs.get(tab);
					Record rec = new Record(new Storable(bucketId),tabNV );
					HWriter.getInstance(multiWriter).insert(tab.toString(), rec);
					ObjectFactory.getInstance().putNVList(tabNV);
				}
			} catch (IOException ex) {
				throw new SystemFault(ex);
			} finally {
				for ( Map<String,byte[]> bucketM : mergedBytes.values()) {
					bucketM.clear();
				}
				mergedBytes.clear();
				tableNVs.clear();
			}
		}
	}	

	/**
	 * 
	 * @param mergedTermTables
	 * @param docTermTable
	 */
	private void buildTermTables( long bucketId,
		Map<Long, TermTables> mergedTermTables, TermTables docTermTable) {
		
		if ( mergedTermTables.containsKey(bucketId)) {
			TermTables mtt = mergedTermTables.get(bucketId);
			mtt.addInSameBucket(docTermTable);
		} else {
			mergedTermTables.put(bucketId, docTermTable);
		}
	}
	
	/**
	 * It transforms the term objects to a byte array.
	 * It saves pointed and allows more documents to get merged.
	 * @param allByteValues	All Bytes
	 * @param mtt	The merged term tables
	 */
	private void flushIntermediate( Map<Long, Map<String,byte[]>> 
		allByteValues, Map<Long, TermTables> mtt) {
		
		if ( null == mtt) return;
		if ( 0 == mtt.size()) return;

		long s = System.currentTimeMillis();
		StringBuilder sb = new StringBuilder(10);
		for (long bucketId : mtt.keySet()) {
			
			TermTables termTables = mtt.get(bucketId);
			for ( char tableName : termTables.tables.keySet()) {
				TermFamilies tf = termTables.tables.get(tableName);
				for (char family : tf.families.keySet()) {
					TermColumns tcs = tf.families.get(family);
					for (char col : tcs.columns.keySet()) {
						sb.append(tableName).append(family).append(col);
						String tfc = sb.toString();
						sb.delete(0, 10);
						TermList tl = tcs.columns.get(col);
						Map<String,byte[]> bytesV = null;
						if ( allByteValues.containsKey(bucketId)) {
							bytesV = allByteValues.get(bucketId);
							if (bytesV.containsKey(tfc)) {
								byte[] tlB = bytesV.get(tfc);
								tl.setExistingBytes(tlB);
								byte[] newB = tl.toBytes();
								bytesV.put(tfc, newB);
								tlB = null;
							} else {
								bytesV.put(tfc, tl.toBytes());
							}
						} else {
							bytesV = new HashMap<String, byte[]>(1);
							bytesV.put(tfc, tl.toBytes());
							allByteValues.put(bucketId, bytesV);
						}
						
						tl.cleanup();
					}
				}
			}
			termTables.cleanup();
		}
		bufferSize = 0;
		if ( InpipeLog.l.isDebugEnabled()) InpipeLog.l.debug(
				"SaveToIndexBatch > flushIntermediate Executation Time " + 
				(System.currentTimeMillis() - s) + " ms");
	}	

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.docMergeFactor = 
			conf.getInt("index.documents.merge", 10000);
		
		this.isIdMappingEnabled = conf.getBoolean("idmapping.enable", true);
	}

	public PipeIn getInstance() {
		return new SaveToIndexBatch(this.docMergeFactor, this.isIdMappingEnabled);
	}

	public String getName() {
		return "SaveToIndexBatch";
	}
	
	/**
	 * Creates a string representation of the table.
	 * @param mergedTermTables
	 * @return
	 */
	private String  printMergedTT(Map<Long, TermTables> mergedTermTables) {
		StringBuilder sb = new StringBuilder();
		for (long bucket : mergedTermTables.keySet()) {
			sb.append("Bucket:").append(bucket);
			TermTables tt =  mergedTermTables.get(bucket);
			for (char table: tt.tables.keySet()) {
				sb.append("\n\tTable:").append(table);
				TermFamilies tf = tt.tables.get(table);
				for (char family : tf.families.keySet()) {
					sb.append("\n\t\tfamily:").append(family);
					TermColumns tc = tf.families.get(family);
					for (char col : tc.columns.keySet()) {
						sb.append("\n\t\t\tColumn:").append(col);
						sb.append(tc.columns.get(col).toString());
					}
				}
			}
		}
		return sb.toString();
	}	

}
