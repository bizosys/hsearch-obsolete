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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.IUpdatePipe;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.schema.EnglishMap;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

/**
 * Delete terms from the inverted index.
 * @author karan
 *
 */
public class DeleteFromIndex implements PipeIn {

	private static final boolean DEBUG_ENABLED = InpipeLog.l.isDebugEnabled();
	List<Doc> documents = null;
	boolean persistId = true;
	
	public DeleteFromIndex() {
	}
	
	public DeleteFromIndex(boolean persistId) {
		this.persistId = persistId;
	}

	public void visit(Object objDoc, boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == objDoc) throw new ApplicationFault("No document");
		if ( null == documents) documents = ObjectFactory.getInstance().getDocumentList();
		documents.add((Doc)objDoc);
	}

	/**
	 * Cuts out section of docpositions which are in the removal list.
	 */
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == this.documents) return;

		Doc curDoc = null; 
		Set<Long> uniqueBuckets = null;
		List<Short> docPositions = null;
		
		try {

			Map<Character,StringBuilder> tables = new HashMap<Character,StringBuilder>();
			EnglishMap map = new EnglishMap();
			
			uniqueBuckets = ObjectFactory.getInstance().getLongSet();
			docPositions = ObjectFactory.getInstance().getShortList();
			
			this.populateUniqueBuckets(uniqueBuckets);
			
			for (long bucket : uniqueBuckets) {
				docPositions.clear();
				tables.clear();
				
				for (Doc aDoc : documents) {
					curDoc = aDoc;
					if ( bucket != aDoc.bucketId) continue;
					if ( null == aDoc.docSerialId) {
						InpipeLog.l.warn("DeleteFromIndex:commit() Skipping Found Null SerialId" + aDoc.toString());
						continue;
					}
					docPositions.add(aDoc.docSerialId);
					
					if ( null == aDoc.terms.all) {
						InpipeLog.l.warn("No terms Found" + curDoc.toString());
						continue;
					}
					buildIndexFields(aDoc, tables, map);
				}
				
				IUpdatePipe pipe = new DeleteFromIndexWithCut(docPositions);
				byte[] pk = Storable.putLong(bucket);
				
				for (Character c : tables.keySet()) {
					String t = c.toString();
					String strFamilies = tables.get(c).toString();
					char[] charFamilies = strFamilies.toCharArray();
					byte[][] families = new byte[charFamilies.length][];
					for ( int i=0; i<charFamilies.length; i++) {
						families[i] = new byte[] { (byte) charFamilies[i]};
					}
					if ( DEBUG_ENABLED ) InpipeLog.l.debug(
						"DeleteFromIndex> Deleting table " + t + " families " + strFamilies);
					HWriter.getInstance(multiWriter).update(t, pk, pipe, families);
				}
			}

			//	Delete the mapping too..
			if ( this.persistId ) {
				for (Doc aDoc : documents) {
					IdMapping.delete(aDoc.tenant, aDoc.teaser.id, multiWriter);
				}
			}
			
		} catch (Exception ex) {
			if ( null != curDoc) InpipeLog.l.error(curDoc.toString(), ex);
			else InpipeLog.l.error(ex);
			
			throw new SystemFault(ex);
		} finally {
			if ( null != this.documents) 
				ObjectFactory.getInstance().putDocumentList(this.documents);
			if ( null != uniqueBuckets) 
				ObjectFactory.getInstance().putLongSet(uniqueBuckets);			
		}
	}

	private void buildIndexFields(Doc curDoc, Map<Character, StringBuilder> tables, EnglishMap map) {
		/**
		 * Build table and family based on terms
		 */
		for (Term aTerm : curDoc.terms.all) {
			if ( StringUtils.isEmpty(aTerm.term)) continue;
			char table = map.getTableName(aTerm.term);
			char family = map.getColumnFamily(aTerm.term);
			//char col = map.getColumn(aTerm.term); Not needed to fetch the row.
			if ( tables.containsKey(table)) {
				StringBuilder sb = tables.get(table); 
				if ( -1 == sb.toString().indexOf(family) ) sb.append(family); 
			} else {
				StringBuilder sb = new StringBuilder();
				sb.append(family);
				tables.put(table, sb);
			}
		}
	}
	
	private  void populateUniqueBuckets(Set<Long> uniqueBuckets) throws ApplicationFault {
		if ( null == this.documents) return;
		for (Doc doc : this.documents) {
			if ( null == doc) continue;
			uniqueBuckets.add(doc.bucketId);
		}
	}
	

	public void init(Configuration conf){
		this.persistId = conf.getBoolean("idmapping.enable", false);
	}

	public PipeIn getInstance() {
		return new DeleteFromIndex(this.persistId);
	}

	public String getName() {
		return "DeleteFromIndex";
	}
}
