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

import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;
import com.bizosys.oneline.util.StringUtils;

/**
 * This ranking runs on the result set which we send to screen for display.
 * So a record may appear towards the first rather than end of the page.
 * The whole ranking does not affect how we reached formed our first page.
 * @author karan
 *
 */
public class ScoreOnTitleMatch implements PipeOut{
	
	public ScoreOnTitleMatch() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		QueryResult res = query.result;
		if ( null == res) return;
		if ( null == res.teasers) return;
		
		String pristineIntent = null; 
		if ( null != query.planner.mustTerms) {
			for ( QueryTerm term : query.planner.mustTerms) {
				if( ! StringUtils.isEmpty(term.termType) ) continue;
				if ( null == pristineIntent) pristineIntent = term.wordOrigLower;
				else pristineIntent = pristineIntent + " " + term.wordOrigLower;
			}
		}
		
		if ( null != query.planner.optionalTerms) {
			for ( QueryTerm term : query.planner.optionalTerms) {
				if( ! StringUtils.isEmpty(term.termType) ) continue;
				if ( null == pristineIntent) pristineIntent = term.wordOrigLower;
				else pristineIntent = pristineIntent + " " + term.wordOrigLower;
			}
		}
		if ( null == pristineIntent) return;
		
		boolean isChanged = false;
		for (Object dtwO : res.teasers) {
			DocTeaserWeight dtw = (DocTeaserWeight)dtwO;
			if ( null == dtw) continue;
			
			//Do we see this in the title ? Reward It
			if ( null != dtw.title) {
				if ( dtw.title.toLowerCase().contains(pristineIntent)) {
					isChanged = true;
					dtw.weight = dtw.weight * 2;
				}
			}
			
			//Do we see this in the Cache Text ? Reward It
			if ( null != dtw.cacheText) {
				isChanged = true;
				if ( dtw.cacheText.toLowerCase().contains(pristineIntent)) 
					dtw.weight = dtw.weight * (3/2);
			}
		}
		
		if ( isChanged ) DocTeaserWeight.sort(res.teasers);
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}
	
	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}

	public PipeOut getInstance() {
		return this;
	}

	public String getName() {
		return "ScoreOnTitleMatch";
	}		
}
