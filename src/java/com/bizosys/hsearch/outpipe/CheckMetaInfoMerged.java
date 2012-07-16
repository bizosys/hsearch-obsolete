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

import com.bizosys.hsearch.common.AccessControl;
import com.bizosys.hsearch.filter.AMFilterCommon;
import com.bizosys.hsearch.filter.AMFilterMerged;
import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.filter.AccessStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryLog;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.SystemFault;

/**
 * This implements callable interface for execution in parallel
 * This actually executes and fetches IDs from the HBase table.
 * @author karan
 *
 */
class CheckMetaInfoMerged {
	
	private static final boolean DEBUG_ENABLED = QueryLog.l.isDebugEnabled();
	private AMFilterMerged pf;
	
	protected CheckMetaInfoMerged(QueryContext ctx) {
		
		AccessStorable aclB = null;
		if ( null == ctx.user ) {
			Access access = new Access();
			access.addAnonymous();
			aclB = access.toStorable();
		} else aclB = AccessControl.getAccessControl(ctx.user).toStorable();
		
		byte[] tagB = ( ctx.matchTags) ? 
				new Storable(ctx.queryString.toLowerCase()).toBytes() : null;
		byte[] stateB = ( null == ctx.state ) ? null : ctx.state.toBytes();
		byte[] teamB = ( null == ctx.team ) ? null : ctx.team.toBytes();
		long ca = ( null == ctx.createdAfter ) ? -1 : ctx.createdAfter.longValue();
		long cb = ( null == ctx.createdBefore ) ? -1 : ctx.createdBefore.longValue();
		long ma = ( null == ctx.modifiedAfter ) ? -1 : ctx.modifiedAfter.longValue();
		long mb = ( null == ctx.modifiedBefore ) ? -1 : ctx.modifiedBefore.longValue();
		
		AMFilterCommon setting = new AMFilterCommon(aclB,
			tagB, stateB, teamB, cb, ca, mb, ma );
		
		this.pf = new AMFilterMerged(setting);
	}
	
	protected List<DocMetaWeight> filter(Object[] staticL, 
		int  scroll, int pageSize ) throws SystemFault {
		
		QueryLog.l.debug("CheckMetaInfoMerged > Call START");
		if ( null == this.pf) return null;
		
		/**
		 * Bring the pointer to beginning from the end
		 */
		int staticT = staticL.length;
		if ( staticT < scroll) return null;
		
		
		/**
		 * Step 1 Identify table, family and column
		 */
		String tableName = IOConstants.TABLE_PREVIEW;
		byte[] familyName = IOConstants.SEARCH_BYTES;
		
		/**
		 * Step 2 Configure Filtering mechanism 
		 */
		HTableWrapper table = null;
		HBaseFacade facade = null;

		List<DocMetaWeight> foundDocs = new ArrayList<DocMetaWeight>();
		try {

			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			DocWeight dw = null;
			
			int available = staticT - scroll;
			if ( available <= 0 ) return null;
			int fetchSize = ( pageSize < available ) ?   pageSize : available ;
			int fetchStart = scroll; // Starts from 0
			int fetchEnd = scroll + fetchSize - 1; //Counting from 0
			if ( DEBUG_ENABLED ) 
				QueryLog.l.debug( "CheckMetaInfoMerged > available:" + available + " , fetchSize" + fetchSize + " ,fetchStart=" + fetchStart + " ,fetchEnd=" + fetchEnd);
			int founds = 0;
			int totalDocs =0; 
			
			Map<Long, Integer> buckets = ( fetchSize > 14 ) ? 
				new HashMap<Long, Integer>(14): new HashMap<Long, Integer>(fetchSize) ;
			
			long tempBucket = -1;
			while ( fetchSize > 0) { //Keep on fetching

				buckets.clear();

				tempBucket = -1;
				
				StringBuilder sb = null;
				if ( DEBUG_ENABLED ) sb = new StringBuilder();
				
				/**
				 * Identify unique buckets and # docs inside it. 
				 * This helps to fetch the merged bytes
				 */
				for (int i=fetchStart; i<= fetchEnd; i++ ) {
					dw = (DocWeight) staticL[i];
					if ( DEBUG_ENABLED ) sb.append(dw.bucketId).append(',');
					tempBucket = dw.bucketId;					
					if ( buckets.containsKey(tempBucket)) {
						buckets.put(tempBucket, buckets.get(tempBucket) + 1); 
					} else {
						buckets.put(tempBucket, 1); 
					}
				}
				if ( DEBUG_ENABLED ) {
					QueryLog.l.debug( "ChechMetaInfoMerged > Found Doc Ids:" + sb.toString());
					sb.delete(0, sb.capacity());
					QueryLog.l.debug("CheckMetaInfoMerged > Distinct Buckets " + buckets.size());
				}
				
				/**
				 * Now create the document list
				 */
				for (long bucket : buckets.keySet()) {
					int docs = buckets.get(bucket);
					if ( 0 == docs) continue;
					int[] serials = new int[docs];
					float[] termWeights = new float[docs];

					if ( DEBUG_ENABLED ) QueryLog.l.debug(
							"CheckMetaInfoMerged > Bucket/Documents(#)  : " + bucket + '/' + docs);
					
					/**
					 * Now iterate through and populate this docSerial
					 */
					int pos = 0;
					
					for (int i=fetchStart; i<= fetchEnd; i++ ) {
						dw = (DocWeight) staticL[i];
						if (dw.bucketId.compareTo(bucket) != 0) continue;
						if ( DEBUG_ENABLED ) sb.append(dw.serialId).append(',');						
						serials[pos] = dw.serialId;
						termWeights[pos] = dw.wt;
						pos++;
						docs--; if ( docs == 0 ) break;
					}
					
					if ( DEBUG_ENABLED ) {
						QueryLog.l.debug( "CheckMetaInfoMerged > Doc SerialIds:" + sb.toString());
						sb.delete(0, sb.capacity());
					}
					
					/**
					 * Load the Document Meta Data From the merged Bytes
					 */
					this.pf.setDocSerials(serials);
					Get getter = new Get(Storable.putLong(bucket));
					getter = getter.addColumn(familyName,IOConstants.META_HEADER);
					getter = getter.addColumn(familyName,IOConstants.META_DETAIL);
					getter = getter.addColumn(familyName,IOConstants.ACL_HEADER);
					getter = getter.addColumn(familyName,IOConstants.ACL_DETAIL);
					//getter = getter.addFamily(familyName);
					getter = getter.setFilter(this.pf);
					getter = getter.setMaxVersions(1);
					
					Result result = table.get(getter);
					byte[] metaHeader = result.getValue(familyName,IOConstants.META_HEADER);
					if ( null == metaHeader) continue;
					byte[] metaData = result.getValue(familyName,IOConstants.META_DETAIL);
					if ( null == metaData) continue;

					totalDocs = metaHeader.length / 2;
					short docPos = 0;
					int beginPos = 0;
					int serialsT = serials.length;
					float aTermWeight;
					for ( int x=0; x< totalDocs; x++) {
						docPos = Storable.getShort(x * 2, metaHeader);
						
						//Find the term weight
						aTermWeight = 0;
						for (int i = 0; i < serialsT; i++) {
							if ( serials[i] == docPos ) {
								//serials[i] = serials[serialsT - 1]; //Bring last here
								//serialsT--;
								aTermWeight = termWeights[i];
								break;
							}
						}
						DocMetaWeight docMeta = new DocMetaWeight(bucket, docPos, aTermWeight);
						beginPos = docMeta.fromBytes(metaData, beginPos);
						foundDocs.add(docMeta);
						founds++;
					}					
					if ( DEBUG_ENABLED ) QueryLog.l.debug(
						"CheckMetaInfoMerged > " + bucket + 
						" Bucket has total documents " + founds + "/" + totalDocs);
				}
				
				fetchStart = fetchEnd + 1; //Next Set beginning
				pageSize = pageSize - founds; //Rest we need 
				available = staticT - fetchStart;
				if ( available == 0 ) break;
				fetchSize = ( pageSize < available ) ?   pageSize : available;
				fetchEnd = fetchStart + fetchSize - 1; //Boundary counting
			}

			return foundDocs;
			
		} catch ( IOException ex) {
			QueryLog.l.fatal("CheckMetaInfoHBase:", ex);
			throw new SystemFault(ex);
		} finally {
			if ( null != table ) facade.putTable(table);
		}	
	}
}