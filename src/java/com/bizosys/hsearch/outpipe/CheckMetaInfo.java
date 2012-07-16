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

import java.util.List;

import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Filters and Ranks on Meta information
 * @author karan
 *
 */
public class CheckMetaInfo implements PipeOut{
	
	int DEFAULT_RETRIEVAL_SIZE = 100;
	boolean dynamicRanking = true;
	
	public CheckMetaInfo() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryResult result = query.result;
		QueryContext ctx = query.ctx;

		if ( null == result) return;
		Object[] staticL = result.sortedStaticWeights;
		if ( null == staticL) return;
		
		int pageSize = (-1 == ctx.metaFetchLimit) ? 
				DEFAULT_RETRIEVAL_SIZE : ctx.metaFetchLimit;

		if ( ! this.dynamicRanking ) {
			if ( pageSize > staticL.length) pageSize = staticL.length; 
			Object[] copiedDynamics = new Object[pageSize];
			for ( int i=0; i< pageSize; i++) {
				DocWeight dw = (DocWeight) staticL[i];
				copiedDynamics[i] = (Object) 
					(new DocMetaWeight(dw.bucketId, dw.serialId, dw.wt));
			}
			result.sortedDynamicWeights = copiedDynamics;
			return;
		}
		
		List<DocMetaWeight> dmwL = null;
		CheckMetaInfoMerged hbase = new CheckMetaInfoMerged(ctx);
		dmwL = hbase.filter(staticL, ctx.scroll, pageSize);
		if ( null == dmwL) return;
		result.sortedDynamicWeights = dmwL.toArray();
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}

	public PipeOut getInstance() {
		return this;
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {		
		this.DEFAULT_RETRIEVAL_SIZE = conf.getInt("meta.fetch.limit", 100);
		this.dynamicRanking = conf.getBoolean("dynamic.ranking.enable", true);
	}
	
	public String getName() {
		return "CheckMetaInfo";
	}			
}
