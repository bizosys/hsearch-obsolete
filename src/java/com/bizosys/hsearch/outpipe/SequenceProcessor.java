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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;
import com.bizosys.oneline.services.async.AsyncProcessor;

import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

/**
 * Process each keyword of the given query in multiple steps
 * Must terms gets processes sequentially with search with in IDs
 * Multiple Optional terms gets processes parallely.
 * 
 * All non existing IDs are marked as -1.
 * @author karan
 *
 */
public class SequenceProcessor implements PipeOut{
	
	private boolean isParallel = false;
	
	public SequenceProcessor() {
	}	
	
	private QueryTerm getSqlTermList() {
		QueryTerm sqlQuery = new QueryTerm();
		TermList tl = new TermList();
		tl.docPos = new short[]{18};
		tl.totalTerms = tl.docPos.length; 
		sqlQuery.foundIds.put(-9223372036854775807L, tl);
		
		return sqlQuery;
	}

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {

		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		if ( null == planner.sequences) throw new ApplicationFault("No Sequencing.");
		
		try {
			List<byte[]> findWithinBuckets = ctx.getBuckets();
			QueryTerm lastMustQuery = getSqlTermList();
			
			for (List<QueryTerm> step : planner.sequences) {
				int totalTasks = step.size();
				if ( 0 == totalTasks) continue;
				
				if ( 1 == totalTasks) {
					QueryTerm curQuery = step.get(0);
					if ( OutpipeLog.l.isDebugEnabled() ) OutpipeLog.l.debug(
						"Processing a single query on this step." + curQuery.toString());
					
					if ( null != ctx.docTypeCode ) curQuery.docTypeCode = ctx.docTypeCode;
					SequenceProcessorFindHBase hbaseProxy = 
						new SequenceProcessorFindHBase(curQuery,findWithinBuckets);
					if ( null != lastMustQuery ) hbaseProxy.setFilterByIds(lastMustQuery);
					hbaseProxy.call();

					if ( ! curQuery.isOptional ) {
						//	No matching term for a must word.
						if ( hbaseProxy.foundBuckets == null) break;
						if (hbaseProxy.foundBuckets.size() == 0 ) break;
						
						OutpipeLog.l.debug("Must Query found bucket size :"  + hbaseProxy.foundBuckets.size());
						findWithinBuckets = hbaseProxy.foundBuckets;
						lastMustQuery = curQuery;
					} else {
						if ( OutpipeLog.l.isDebugEnabled() )
							OutpipeLog.l.debug("Optional Query.."  + curQuery.toString());
					}
				
				} else { //Lastly multiple Optional terms Process parallely

					if ( OutpipeLog.l.isDebugEnabled() ) OutpipeLog.l.debug("Processing in a parallel step.");
					List<SequenceProcessorFindHBase> findIdJobs = new ArrayList<SequenceProcessorFindHBase>(step.size()); 
					for(QueryTerm term : step) {
						SequenceProcessorFindHBase hbaseProxy = (this.isParallel) ? 
								new SequenceProcessorFindHBaseParallel(term,findWithinBuckets) :
									new SequenceProcessorFindHBase(term,findWithinBuckets); 
						if ( null != lastMustQuery ) hbaseProxy.setFilterByIds(lastMustQuery);
						findIdJobs.add(hbaseProxy);
					}
					AsyncProcessor.getInstance().getThreadPool().invokeAll(findIdJobs);
				}
			}
			
			intersectMustQs(planner, lastMustQuery);
			subsetOptQs(planner, lastMustQuery);
			
		} catch (InterruptedException ex) {
			String msg = ( null == planner) ? "Empty Planner" : planner.toString(); 
			OutpipeLog.l.fatal("Interrupted @ SequenceProcessor > " + msg, ex);
			throw new SystemFault(ex);
		} catch (Exception ex) {
			String msg = ( null == planner) ? "Empty Planner" : planner.toString(); 
			OutpipeLog.l.fatal("Failed @ SequenceProcessor > " + msg, ex);
			throw new SystemFault(ex);
		}
	}

	/**
	 * This subsets across all MUST queries.
	 * Last 2 must queries are already in sync from the processing.
	 * @param planner
	 * @param lastMustQuery
	 */
	private void intersectMustQs(QueryPlanner planner, QueryTerm lastMustQuery) {
		if ( null == lastMustQuery) return;
		int stepsT = planner.sequences.size();
		
		for ( int step = stepsT - 1; step > -1; step--) {

			/**
			 * More than 1 means optional
			 */
			List<QueryTerm> curStepQueries = planner.sequences.get(step);
			if ( curStepQueries.size() != 1) continue; 
			
			/**
			 * Look for must only
			 */
			QueryTerm curQuery = curStepQueries.get(0);
			if ( curQuery.isOptional) continue;
			
			/**
			 * Last must query - Already processed
			 */
			if ( lastMustQuery == curQuery) continue;

			/**
			 * Remove the buckets which are absent and then IDs
			 */
			Map<Long, TermList> curResultBuckets = curQuery.foundIds;
			Map<Long, TermList> lastQueryBuckets = lastMustQuery.foundIds;
			int curBucketsT = curResultBuckets.size();
			if ( curBucketsT == 0 ) {
				if ( OutpipeLog.l.isDebugEnabled() ) {
					OutpipeLog.l.debug("No found items for the must query.");
				}
				lastQueryBuckets.clear();
				return;
			}
			
			Iterator<Long> curBucketsItr = curResultBuckets.keySet().iterator();
			for ( int i=0; i<curBucketsT; i++ ) {
				Long bucketId = curBucketsItr.next();
				boolean hasElements = lastQueryBuckets.containsKey(bucketId);
				if ( hasElements) {
					hasElements = curResultBuckets.get(bucketId).
						intersect(lastQueryBuckets.get(bucketId));
					if ( ! hasElements) {
						curBucketsItr.remove();
						lastQueryBuckets.remove(bucketId);
					}
				} else {
					curBucketsItr.remove();
				}
			}
		}
	}
	
	/**
	 * This subsets across all MUST queries.
	 * Last 2 must queries are already in sync from the processing.
	 * @param planner
	 * @param lastMustQuery
	 */
	private void subsetOptQs(QueryPlanner planner, QueryTerm lastMustQuery) {
		if ( null == lastMustQuery) return;
		int stepsT = planner.sequences.size();
		for ( int step = stepsT -1; step > -1; step--) {
			List<QueryTerm> curStep = planner.sequences.get(step);
			for (QueryTerm curQuery : curStep) {
				if ( !curQuery.isOptional) continue;
				
				/**
				 * Remove the buckets which are absent and then IDs
				 */
				Map<Long, TermList> curBuckets = curQuery.foundIds;
				Map<Long, TermList> lastBuckets = lastMustQuery.foundIds;
				int curBucketsT = curBuckets.size();
				Iterator<Long> curBucketsItr = curBuckets.keySet().iterator();
				for ( int i=0; i<curBucketsT; i++ ) {
					Long bucketId = curBucketsItr.next();
					boolean hasElements = lastBuckets.containsKey(bucketId);
					if ( hasElements) {
						hasElements = curBuckets.get(bucketId).subset(lastBuckets.get(bucketId));
						if ( !hasElements) curBucketsItr.remove();
					} else {
						curBucketsItr.remove();
					}
				}
			}
		}
	}	
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}

	public PipeOut getInstance() {
		return this;
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.isParallel = conf.getBoolean("parallelization", false);
	}
	
	public String getName() {
		return "SequenceProcessor";
	}		
}