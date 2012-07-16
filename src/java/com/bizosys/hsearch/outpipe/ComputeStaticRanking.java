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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.query.DocWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Static ranking is done based on term weight
 * @author karan
 *
 */
public class ComputeStaticRanking implements PipeOut{
	
	Map<String, DocWeight> docWeightMap = null;
	int highGradesLimit = 1000;
	
	/**
	 * DEfault Constructor follows with a init
	 */
	public ComputeStaticRanking() {
	}
	
	/**
	 * Subsequent constructor.. Initializes with default settings
	 * @param dynamicRanked
	 */
	public ComputeStaticRanking(int dynamicRanked) {
		this.highGradesLimit = dynamicRanked;
	}

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		OutpipeLog.l.debug("ComputeStaticRank ENTER");
		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		QueryResult result = query.result;
		
		Collection<DocWeight> highGrades = computeWeight(ctx, planner);
		if ( OutpipeLog.l.isDebugEnabled()) {
			if ( null == highGrades) OutpipeLog.l.debug("ComputeStaticRank NONE");
			else OutpipeLog.l.debug("ComputeStaticRank TOTAL = " + highGrades.size());
		}

		gradeBasedSorting(result, highGrades);
		highGrades.clear();
		highGrades = null;
	}
	
	/**
	 * Compute the static weight
	 * @param ctx
	 * @param planner
	 * @return
	 */
	private Collection<DocWeight> computeWeight(QueryContext ctx, QueryPlanner planner) {
		
		float thisWt = -1;
		String mappedDocId = null;
		String idPrefix = "";
		
		this.docWeightMap = ObjectFactory.getInstance().getDocWeightMap();

		for ( List<QueryTerm> qts : planner.sequences) {
			if ( null == qts) continue;
			
			for ( QueryTerm qt : qts) {			
				if ( null == qt) continue;
				Map<Long, TermList> founded = qt.foundIds;
				if ( null == founded) continue;
				for ( Long bucket: founded.keySet()) {
					TermList tl = founded.get(bucket);
					if ( null == tl) continue;
					idPrefix = bucket.toString() + "_";

					int bytePos = -1;
					for ( short docPos : tl.docPos ) {
						if ( -1 == docPos) continue;
						bytePos++;
						thisWt = tl.termWeight[bytePos];
						if ( thisWt < 0 ) thisWt = 0;
						if ( qt.preciousNess > 0) thisWt = thisWt * qt.preciousNess;
						thisWt = thisWt * ctx.boostTermWeight;
						
						mappedDocId = idPrefix + docPos;
						if ( docWeightMap.containsKey(mappedDocId) ) {
							docWeightMap.get(mappedDocId).add(thisWt); 
						} else {
							docWeightMap.put(mappedDocId, new DocWeight(bucket, docPos, thisWt) ); 								
						}
					}
					tl.cleanup();
				}
				founded.clear();
			}
		}
		planner.sequences.clear();
		return docWeightMap.values();
	}	
		

	/**
	 * Deduct Maximum and minimum range of the document weights
	 * @param values	Document Weight Collection
	 * @return	min,max values as array
	 */
	private float[] getMinMaxScore(Collection<DocWeight> values) {
		float max = -999999.00F;
		float min = 999999.00F;
		
		for (DocWeight weight : values) {
			if ( weight.wt > max) max = weight.wt;
			if ( min > weight.wt) min = weight.wt; 
		}	
		return new float[] { min, max };
	}
	
	/**
	 * Grade to 0-10 based on weight ranges computed based on max and min value
	 * @param values	DocWeight collections
	 * @param minLimit	Maximum Weight
	 * @param maxLimit	Minimum Weight
	 * @return
	 */
	private Collection<DocWeight> keepHighGrades(
		Collection<DocWeight> values, float minLimit, float maxLimit) {
		
		if ( OutpipeLog.l.isInfoEnabled()) OutpipeLog.l.info(
			"Static Ranking Range Min/Max: " + minLimit + "/" + maxLimit );
		
		if ( minLimit == maxLimit) return values;
		float diff = (maxLimit - minLimit) / 10;
		float[] gradesRanges = new float[] {minLimit, minLimit + diff, 
		    minLimit + 2 * diff, minLimit + 3 * diff, minLimit + 4 * diff,
		    minLimit + 5 * diff, minLimit + 6 * diff, minLimit + 7 * diff,
		    minLimit + 8 * diff, minLimit + 9 * diff };
		
		int[] gradesTotals = new int[]{0,0,0,0,0,0,0,0,0,0}; 
		
		for (DocWeight weight : values) {
			if (weight.wt <= gradesRanges[1]) gradesTotals[0]++;
			else if (weight.wt <= gradesRanges[2]) gradesTotals[1]++;
			else if (weight.wt <= gradesRanges[3]) gradesTotals[2]++;
			else if (weight.wt <= gradesRanges[4]) gradesTotals[3]++;
			else if (weight.wt <= gradesRanges[5]) gradesTotals[4]++;
			else if (weight.wt <= gradesRanges[6]) gradesTotals[5]++;
			else if (weight.wt <= gradesRanges[7]) gradesTotals[6]++;
			else if (weight.wt <= gradesRanges[8]) gradesTotals[7]++;
			else if (weight.wt <= gradesRanges[9]) gradesTotals[8]++;
			else gradesTotals[9]++;
		}
		
		int total = 0;
		int cutoffIndex;
		for (cutoffIndex=9; cutoffIndex>-1; cutoffIndex-- ) {
			total = total + gradesTotals[cutoffIndex];
			if ( total > highGradesLimit) break;
		}
		if ( cutoffIndex == -1) return values; //Included All
		
		//Remove all the low grades
		Iterator<DocWeight> valuesI = values.iterator();
		int valuesT = values.size();
		for ( int j=0; j<valuesT; j++ ) {
			DocWeight weight = valuesI.next();
			if ( weight.wt <= gradesRanges[cutoffIndex]) 
				valuesI.remove();
			valuesT--;
			j--;
		}
		return values;
	}
	
	private void gradeBasedSorting(QueryResult result, Collection<DocWeight> highGrades) throws SystemFault {
		float[] minMax = null;
		while ( true) {
			minMax = getMinMaxScore(highGrades);
			if ( minMax[0] == minMax[1]) break;
			
			int existingT = highGrades.size();
			highGrades = keepHighGrades(highGrades,minMax[0],minMax[1]);
			int newT = highGrades.size();
			
			if ( OutpipeLog.l.isDebugEnabled()) OutpipeLog.l.debug(
				"ComputeStaticRank Gradation :" + existingT + "/" + newT);
			if ( existingT == newT) break;
		}
		result.sortedStaticWeights = highGrades.toArray();
		if ( minMax[0] != minMax[1] ) DocWeight.sort(result.sortedStaticWeights);
	}
		

	public boolean commit() throws ApplicationFault, SystemFault {
		ObjectFactory.getInstance().putDocWeightMap(docWeightMap);
		return true;
	}

	public PipeOut getInstance() {
		return new ComputeStaticRanking(this.highGradesLimit);
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.highGradesLimit = conf.getInt("meta.fetch.limit", 100) * 2;
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}
	
	public String getName() {
		return "ComputeStaticRanking";
	}		
}
