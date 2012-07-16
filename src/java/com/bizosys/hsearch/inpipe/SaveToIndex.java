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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.index.IndexLog;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermColumns;
import com.bizosys.hsearch.index.TermFamilies;
import com.bizosys.hsearch.index.TermTables;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.schema.SchemaManager;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.RecordScalar;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Saves the term vector to the index table
 * @author karan
 *
 */
public class SaveToIndex implements PipeIn {

	int docMergeFactor = 1000;
	boolean isIdMappingEnabled = true;
	static boolean isDebug = InpipeLog.l.isDebugEnabled();

	/** Arranged by document */
	Map<Doc, TermTables> docTermTables = null;
	
	public SaveToIndex() {
		
	}
	
	public SaveToIndex(int docMergeFactor, boolean isIdMappingEnabled) {
		this.docMergeFactor = docMergeFactor;
		this.isIdMappingEnabled = isIdMappingEnabled;
	}

	public void visit(Object docObj, boolean concurrency) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;

		if ( null == doc.terms) return;
		if ( null == doc.terms.all) return;
		
		ILanguageMap map = SchemaManager.getInstance().getLanguageMap(doc.meta.locale);
		TermTables termTable = ( null == doc.bucketId ) ?
			new TermTables(concurrency) : new TermTables( new Storable(doc.bucketId),concurrency);
			
		for (Term term : doc.terms.all) {
			term.setDocumentPosition(doc.docSerialId);
			termTable.add(term, map);
		}
		doc.terms.closeTermList();
		if ( null == this.docTermTables) this.docTermTables = 
			ObjectFactory.getInstance().getDocTermTable();
		this.docTermTables.put(doc, termTable);
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {

		if ( null == this.docTermTables) return;
		
		/**
		 * We need to arrange all terms from documents to arrange in term buckets.
		 */
		Map<Long, TermTables> mergedTermTables = 
			ObjectFactory.getInstance().getBucketTermTable();
		
		/**
		 * Check of any document does not have the document serial Ids
		 */
		List<IdMapping> docMappedIds = null;
		
		for (Doc aDoc : this.docTermTables.keySet() ) {
			TermTables docTermTable = this.docTermTables.get(aDoc);
			//Sanity check. Expecting bucket Id for all.
			if ( null == docTermTable.bucketId ) 
				throw new ApplicationFault ("SaveToIndex >> Bucket Id Missing." );
			
			if (isDebug) InpipeLog.l.debug("Build term tables and assign document position");
			buildTermTables(mergedTermTables, docTermTable);
			docTermTable.assignDocumentPosition(aDoc.docSerialId);
			
			if (isDebug) InpipeLog.l.debug("Id Mapping, Necessary for mapping from the original Key");
			if ( null == docMappedIds) docMappedIds = new ArrayList<IdMapping>();
			docMappedIds.add(new IdMapping(aDoc.tenant, aDoc.teaser.id,aDoc.bucketId,aDoc.docSerialId));

			if (isDebug) InpipeLog.l.debug("Dedup Terms");
			buildTermTables(mergedTermTables, docTermTable);
		}
		
		if ( IndexLog.l.isDebugEnabled()) IndexLog.l.debug(printMergedTT(mergedTermTables));

		/**
		 * Persist Ids
		 */
		if ( null != docMappedIds && isIdMappingEnabled) {
			if (isDebug) InpipeLog.l.debug("Persisting Id Mappings");
			List<RecordScalar> mapRecords = ObjectFactory.getInstance().getScalarRecordList();
			for (IdMapping mapping : docMappedIds) {
				mapping.build(mapRecords);
			}
			IdMapping.persist(mapRecords,multiWriter);
			ObjectFactory.getInstance().putScalarRecordList(mapRecords);
		}

		/**
		 * Persist Terms
		 */
		if ( 0 == mergedTermTables.size()) return;
		for (Long bucketId : mergedTermTables.keySet()) {
			if (isDebug) InpipeLog.l.debug("Persisting terms : " + bucketId);
			TermTables termTables = mergedTermTables.get(bucketId);
			termTables.persist(true, false);
			termTables.cleanup();
		}
		
		cleanup(mergedTermTables);
	}

	private void cleanup(Map<Long, TermTables> mergedTermTables) {
		/**
		 * Clear the resources.
		 */
		if ( null != docTermTables) {
			for (TermTables tt : docTermTables.values()) {
				if ( null != tt) tt.cleanup();
			}
			ObjectFactory.getInstance().putDocTermTable(docTermTables);
		}
		if ( null != mergedTermTables) {
			for (TermTables tt : mergedTermTables.values()) {
				if ( null != tt) tt.cleanup();
			}
			ObjectFactory.getInstance().putBucketTermTable(mergedTermTables);
		}
	}



	/**
	 * 
	 * @param mergedTermTables
	 * @param docTermTable
	 */
	private void buildTermTables(
		Map<Long, TermTables> mergedTermTables, TermTables docTermTable) {
		
		byte[] bucketIdB = docTermTable.bucketId.toBytes();
		long bucketId = Storable.getLong(0, bucketIdB);
		
		if ( mergedTermTables.containsKey(bucketId)) {
			TermTables mtt = mergedTermTables.get(bucketId);
			mtt.add(docTermTable);
		} else {
			mergedTermTables.put(bucketId, docTermTable);
		}
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.docMergeFactor = 
			conf.getInt("index.documents.merge", 10000);
		
		this.isIdMappingEnabled = conf.getBoolean("idmapping.enable", false);
	}

	public PipeIn getInstance() {
		return new SaveToIndex(this.docMergeFactor, this.isIdMappingEnabled);
	}

	public String getName() {
		return "SaveToIndex";
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
