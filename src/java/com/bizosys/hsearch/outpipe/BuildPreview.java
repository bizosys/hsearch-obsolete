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

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Builds teaser information
 * @author karan
 *
 */
public class BuildPreview implements PipeOut{
	
	int pageSize = 10;
	int teaserLength = 20;
	boolean parallelProcessed = true;	
	
	public BuildPreview() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryResult res = query.result;
		if ( null == res) return;
		if ( null == res.sortedDynamicWeights) return;
		
		int documentFetchLimit = (-1 == ctx.documentFetchLimit) ? 
			pageSize : ctx.documentFetchLimit;
		
		int teaserCutSection = (-1 == ctx.teaserSectionLen) ?
			teaserLength : ctx.teaserSectionLen;
		
		int foundT = res.sortedDynamicWeights.length;
		
		int maxFetching = ( documentFetchLimit <  foundT) ? 
				documentFetchLimit : foundT;
		
		List<DocTeaserWeight> weightedTeasers = new ArrayList<DocTeaserWeight>(maxFetching);
		
		/**
		 * Make array list of words
		 */
		int termsMT = ( null == query.planner.mustTerms) ? 0 : query.planner.mustTerms.size();
		int termsOT = ( null == query.planner.optionalTerms) ?
			0 : query.planner.optionalTerms.size();
		byte[][] wordsB = new byte[termsMT + termsOT][];
		for ( int i=0; i<termsMT; i++) {
			wordsB[i] = new Storable(query.planner.mustTerms.get(i).wordOrig).toBytes();
		}
		for ( int i=0; i<termsOT; i++) {
			wordsB[i+termsMT] = new Storable(query.planner.optionalTerms.get(i).wordOrig).toBytes();
		}
		
		BuildPreviewMerged btm = new BuildPreviewMerged(wordsB, (short) teaserCutSection);
		weightedTeasers = btm.filter(res.sortedDynamicWeights,maxFetching, parallelProcessed);
		res.teasers = weightedTeasers.toArray();
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}
	
	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.pageSize = conf.getInt("page.fetch.limit", 10);
		this.teaserLength = conf.getInt("teaser.words.count", 100);
		this.parallelProcessed = conf.getBoolean("parallelization", true);
	}

	public PipeOut getInstance() {
		return this;
	}

	public String getName() {
		return "BuildTeaser";
	}		
}
