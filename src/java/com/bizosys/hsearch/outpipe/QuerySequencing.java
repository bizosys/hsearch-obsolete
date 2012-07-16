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
import java.util.Arrays;
import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

/**
 * There are 2 elements
 * 1 - The Must terms / Optional terms
 * 2 - The term preciousness 
 * -------------------------
 * Must 		Optional
 * -------------------------
 * 0 			0				Do nothing
 * 0			1				Convert optional as must, perform search
 * 0			Many			Load all the IDs (Parallel)
 * 1			0				Regular Search				
 * 1			1				Perform Must, Filter Optional
 * 1			Many			Perform Must, Filtered Optionals in parallel
 * Many			0				Sequential must terms on preciousness order 
 * Many			1				Sequential must terms on preciousness order + Filtered Optional
 * Many			Many			Sequential must terms on preciousness order + Filtered Optionals in parallel 
 */
public class QuerySequencing implements PipeOut{
	
	public QuerySequencing() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		HQuery query = (HQuery) objQuery;
		//QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		int mustTermsT = (null == planner.mustTerms) ? 0 : planner.mustTerms.size();
		int optTermsT = (null == planner.optionalTerms) ? 0 : planner.optionalTerms.size();
		
		if ( 0 == mustTermsT) { //Process in 1 step
			if ( 0 == optTermsT) throw new ApplicationFault("No search query present");
			
			planner.sequences = new ArrayList<List<QueryTerm>>(1);
			List<QueryTerm> step0 = new ArrayList<QueryTerm>(1);
			step0.addAll(planner.optionalTerms);
			planner.sequences.add(step0);
			
		} else if ( 1 == mustTermsT) {  //Process in 2 steps
			
			planner.sequences = new ArrayList<List<QueryTerm>>(1);
			List<QueryTerm> step0 = new ArrayList<QueryTerm>(1);
			step0.addAll(planner.mustTerms);
			planner.sequences.add(step0);
			
			if ( 0 != optTermsT) {
				List<QueryTerm> step1 = new ArrayList<QueryTerm>(1);
				step1.addAll(planner.optionalTerms);
				planner.sequences.add(step1);
			}
			
		} else { //Find the precious ones and go in sequences

			planner.sequences = new ArrayList<List<QueryTerm>>(1);
			QueryTerm[] qtL = (QueryTerm[])
				planner.mustTerms.toArray(new QueryTerm[mustTermsT]);
			
			Arrays.sort(qtL, new QueryTerm());
			for (QueryTerm term : qtL) {
				List<QueryTerm> step = new ArrayList<QueryTerm>(1);
				step.add(term);
				if ( OutpipeLog.l.isInfoEnabled()) OutpipeLog.l.info(
					"Adding a Must Processing Step:" + term.toString());
				planner.sequences.add(step);
			}
			
			if ( optTermsT > 0) {
				List<QueryTerm> optStep = new ArrayList<QueryTerm>(1);
				optStep.addAll(planner.optionalTerms);
				if ( OutpipeLog.l.isInfoEnabled()) OutpipeLog.l.info(
					"Adding all optional Processing Step:" + optStep.toString());				
				planner.sequences.add(optStep);
			}
		}
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}

	public PipeOut getInstance() {
		return this;
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}
	
	public String getName() {
		return "QuerySequencing";
	}		
}
