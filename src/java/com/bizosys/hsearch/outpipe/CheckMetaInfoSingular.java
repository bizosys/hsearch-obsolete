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
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import com.bizosys.hsearch.common.AccessControl;
import com.bizosys.hsearch.filter.AMFilterCommon;
import com.bizosys.hsearch.filter.AMFilterSingular;
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
class CheckMetaInfoSingular {
	
	private AMFilterSingular pf;
	
	protected CheckMetaInfoSingular(QueryContext ctx) {
		
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
			tagB, stateB, teamB, cb, ca, mb , ma);
		
		this.pf = new AMFilterSingular(setting);
	}
	
	protected List<DocMetaWeight> filter(Object[] staticL, 
		int  scroll, int pageSize ) throws SystemFault {
		
		QueryLog.l.debug("CheckMetaInfoSingular > Call START");
		if ( null == this.pf) return null;
		
		/**
		 * Bring the pointer to beginning from the end
		 */
		int staticT = staticL.length;
		if ( staticT <= scroll) scroll = 0;
		
		
		/**
		 * Step 1 Identify table, family and column
		 */
		String tableName = IOConstants.TABLE_PREVIEW;
		byte[] familyName = IOConstants.SEARCH_BYTES;
		byte[] metaColumn = IOConstants.META_BYTES;
		
		/**
		 * Step 2 Configure Filtering mechanism 
		 */
		HTableWrapper table = null;
		HBaseFacade facade = null;

		List<DocMetaWeight> foundDocs = new ArrayList<DocMetaWeight>();
		try {

			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			int totalMatched = 0;
			
			for (int i=scroll; i< staticT; i++ ) {
				if ( totalMatched >= pageSize) break; //Just read enough for the page size

				DocWeight dw = (DocWeight) staticL[i];
				byte[] idB = dw.getIdBytes();
				Get getter = new Get(idB);
				getter = getter.addColumn(familyName,metaColumn);
				getter = getter.setFilter(this.pf);
				
				Result result = table.get(getter);
				byte[] metaValue = result.getValue(familyName,metaColumn);
				if ( null == metaValue) continue;
				foundDocs.add(new DocMetaWeight(dw.bucketId, dw.serialId,metaValue));
				totalMatched++;
			}
			
			if ( QueryLog.l.isInfoEnabled() )
				QueryLog.l.info("CheckMetaInfoSingular > Found Documents:" + totalMatched);
			return foundDocs;
			
		} catch ( IOException ex) {
			QueryLog.l.fatal("CheckMetaInfoSingular:", ex);
			throw new SystemFault(ex);
		} finally {
			if ( null != table ) facade.putTable(table);
		}	
	}
}