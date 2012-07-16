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
package com.bizosys.hsearch.outpipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.filter.TeaserFilterMerged;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.QueryLog;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.SystemFault;

/**
 * This implements callable interface for execution in parallel
 * This actually executes and fetches IDs from the HBase table.
 * @author karan
 *
 */
class BuildPreviewMerged {
	
	private TeaserFilterMerged pf;
	
	protected BuildPreviewMerged(byte[][] wordsB, short teaserSize) {
		this.pf = new TeaserFilterMerged(wordsB, teaserSize);
	}
	
	protected List<DocTeaserWeight> filter(Object[] metaL, int pageSize, boolean parallelProcessed ) throws SystemFault {
		
		QueryLog.l.debug("BuildTeaserMerged > Start");
		if ( null == this.pf) return null;
		
		/**
		 * Bring the pointer to beginning from the end
		 */
		int metaT = metaL.length;
		
		
		/**
		 * Step 1 Identify table, family and column
		 */
		String tableName = IOConstants.TABLE_PREVIEW;
		byte[] familyName = IOConstants.TEASER_BYTES;
		
		/**
		 * Step 2 Configure Filtering mechanism 
		 */
		HTableWrapper table = null;
		HBaseFacade facade = null;

		List<DocTeaserWeight> teasers = new ArrayList<DocTeaserWeight>();
		try {

			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			DocMetaWeight dw = null;
			
			int fetchSize = pageSize;
			if ( fetchSize > metaT ) fetchSize = metaT; 
			
			Map<Long, Integer> buckets = null;
			buckets = ( fetchSize < 5 ) ? new HashMap<Long, Integer>(fetchSize) : new HashMap<Long, Integer>();

			/**
			 * Compute how many document available per bucket
			 */
			long tempBucket = -1;
			for (int i=0; i< fetchSize; i++ ) {
				dw = (DocMetaWeight) metaL[i];
				tempBucket = dw.bucketId;
				if ( buckets.containsKey(tempBucket)) {
					buckets.put(tempBucket, buckets.get(tempBucket) + 1); 
				} else {
					buckets.put(tempBucket, 1); 
				}
			}
				
			boolean DEBUG_MODE = QueryLog.l.isDebugEnabled();
			if ( DEBUG_MODE ) QueryLog.l.debug(
				"BuildTeaserMerged > Distinct Buckets " + buckets.size());
				
			List<Row> gets = null;
			if ( parallelProcessed ) gets = new ArrayList<Row>();
				
			/**
			 * Now create the document list
			 */
			for (long bucket : buckets.keySet()) {
				int docs = buckets.get(bucket);
				if ( 0 == docs) continue;
				int[] serials = new int[docs];
				
				/**
				 * Now iterate through and populate this docSerial
				 */
				int pos = 0;
				
				for (int i=0; i< fetchSize; i++ ) {
					dw = (DocMetaWeight) metaL[i];
					if (dw.bucketId.compareTo(bucket) != 0) continue;
					serials[pos] = dw.serialId; pos++;
					docs--; if ( docs == 0 ) break;
				}
				
				Get getter = new Get(Storable.putLong(bucket));
				getter = getter.addColumn(familyName,IOConstants.TEASER_HEADER);
				getter = getter.addColumn(familyName,IOConstants.TEASER_DETAIL);
				this.pf.setDocSerials(serials);
				getter = getter.setFilter(this.pf);
				
				if ( parallelProcessed ) {
					TeaserFilterMerged another = this.pf.clone();
					another.setDocSerials(serials);
					getter = getter.setFilter(another);
					gets.add(getter);
				}
				else {
					if ( DEBUG_MODE ) QueryLog.l.debug("BuildTeaserMerged > Sequential Mode Processing");
					this.pf.setDocSerials(serials);
					Result result = table.get(getter);
					if ( null == result) continue;
					fetch(table,result,familyName,bucket, teasers);
				}
				
			}
			
			if ( null != gets) {
				if ( DEBUG_MODE ) QueryLog.l.debug("BuildTeaserMerged > Parallel Mode Processing");
				Object[] results = table.batch(gets);
				if ( null != results) {
					for (Object resObj : results) {
						if ( null == resObj) continue;
						Result result = (Result) resObj;
						fetch(table,result,familyName,teasers);						
					}
				}
			}
			
			if ( DEBUG_MODE ) {
				int size = ( null == teasers) ? 0 : teasers.size();
				QueryLog.l.debug("BuildTeaserMerged > Total Teasers:" + size);
			}
			
			/**
			 * Assign the static weights as base to dynamic weights  
			 */
			int lastSeq = 0;
			int i=0;
			for (DocTeaserWeight dt : teasers) {
				i=lastSeq;
				for (; i< fetchSize; i++ ) {
					dw = (DocMetaWeight) metaL[i];
					if ( dt.bucketId.compareTo(dw.bucketId) == 0 ) {
						if ( dt.serialId.compareTo(dw.serialId) == 0) {
							dt.weight = dw.weight;
							break;
						}
					}
				}
				if ( lastSeq == i) lastSeq++;
			}
			return teasers;
			
		} catch ( InterruptedException ex) {
			QueryLog.l.fatal("BuildTeaserMerged:", ex);
			throw new SystemFault(ex);
		} catch ( IOException ex) {
			QueryLog.l.fatal("BuildTeaserMerged:", ex);
			throw new SystemFault(ex);
		} finally {
			if ( null != table ) facade.putTable(table);
		}	
	}
	
	public void fetch(HTableWrapper table, Result result,
			byte[] familyName, List<DocTeaserWeight> teasers) throws IOException{
		
		long bucket = Storable.getLong(0, result.getRow());
		fetch(table, result, familyName, bucket, teasers);
	}

	public void fetch(HTableWrapper table, Result result,
			byte[] familyName, long bucket, List<DocTeaserWeight> teasers) throws IOException{
		
		byte[] teaserHeader = result.getValue(familyName,IOConstants.TEASER_HEADER);
		if ( null == teaserHeader) return;
		byte[] teaserData = result.getValue(familyName,IOConstants.TEASER_DETAIL);
		if ( null == teaserData) return;

		int totalDocs = teaserHeader.length / 2; /** Document serial is short */
		int beginPos = 0;
		short docPos = 0;
		for ( short x=0; x< totalDocs; x++) {
			docPos = Storable.getShort(x * 2, teaserHeader);
			DocTeaserWeight docTeaser = new DocTeaserWeight(bucket, docPos);
			beginPos = docTeaser.fromBytes(teaserData, beginPos);
			teasers.add(docTeaser);
		}		
	}
	
}