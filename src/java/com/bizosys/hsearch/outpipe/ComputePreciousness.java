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

import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Preciousness is computed after finding detail about the word from
 * the dictionary. Dictionary maintains number of documents who have 
 * the word in their term vector. More documents containing a word, 
 * the lesser precious it it.
 * @author karan
 *
 */
public class ComputePreciousness implements PipeOut{
	
	public ComputePreciousness() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		HQuery query = (HQuery) objQuery;
		@SuppressWarnings("unused")
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		/**
		 * Go through the list to find which one maximim occuring
		 * Compute based on that from 0-1 scale the preciousness
		 */
		int maxOccurance1 = computeMaximimOccurance(planner.mustTerms);
		int maxOccurance2 = computeMaximimOccurance(planner.optionalTerms);
		int maxOccurance = ( maxOccurance1 > maxOccurance2) ? maxOccurance1 : maxOccurance2;
		/**
		if ( 0 == maxOccurance) {
			OutpipeLog.l.info("Word not recognized " + ctx.queryString);
			throw new ApplicationFault("Word not Recognized : " +  ctx.queryString);
		}
		*/
		computePreciousness(planner.mustTerms, maxOccurance);
		computePreciousness(planner.optionalTerms, maxOccurance);
	}
	
	/**
	 * Compute the maximum occurance instance.  
	 * @param queryWordL
	 * @return
	 * @throws ApplicationFault
	 */
	private int computeMaximimOccurance(List<QueryTerm> queryWordL) 
	throws ApplicationFault {
		
		int maxOccurance = 0;
		if ( null == queryWordL) return 0;
		for (QueryTerm term : queryWordL) {
			if ( null == term.foundTerm) continue;
			if ( term.foundTerm.frequency > maxOccurance)
				maxOccurance = term.foundTerm.frequency;
		}
		return maxOccurance;
	}
	
	/**
	 * 1 is most previous and 0 is least precious
	 * Less found terms are more precious in nature 
	 * @param queryWordL
	 * @param maxOccurance
	 * @throws ApplicationFault
	 */
	private void computePreciousness(List<QueryTerm> queryWordL, 
		int maxOccurance) throws ApplicationFault {
		
		if ( null == queryWordL) return;
		for (QueryTerm term : queryWordL) {
			if ( null == term.foundTerm) continue;
			if ( 0 == maxOccurance ) maxOccurance = 1;
			term.preciousNess = 1 - ( term.foundTerm.frequency / maxOccurance);
			if ( 0 == term.preciousNess) term.preciousNess = 0.01f; 
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
		return "ComputePreciousness";
	}
	
}
