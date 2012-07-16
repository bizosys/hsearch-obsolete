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

import java.util.Date;

import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.util.IpUtil;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

/**
 * Ranks the document based on term and meta information.
 * Following criterias are taken for finding the dynamic ranking
 * <lu>
 * <li>Freshness</li>
 * <li>IP Proximity</li>
 * <li>Sighting on author Tag words</li>
 * <li>Sighting on User Tag words</li>
 * </lu> 
 * @author karan
 *
 */
public class ComputeDynamicRanking implements PipeOut{
	
	private static final boolean DEBUG_ENABLED = OutpipeLog.l.isDebugEnabled();

	public ComputeDynamicRanking() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {		

		HQuery query = (HQuery) objQuery;
		QueryResult result = query.result;
		if ( null == result.sortedDynamicWeights) return;

		QueryContext ctx = query.ctx;
		QueryPlanner plan = query.planner;
		int ipHouse = ( null == ctx.ipAddress ) ? 0 : IpUtil.computeHouse(ctx.ipAddress);
		
		int wtFreshness=0,wtIpProxim=0,wtSocial=0,wtTags=0;

		StringBuilder log = new StringBuilder();
		int logTop10 = 0;
		if ( DEBUG_ENABLED) {
			log.append("wtTerm|wtFreshness|wtIpProxim|wtSocial|wtTags|meta.weight\n");
		}
		float finalWeight = 0;
		
		for (Object metaO : result.sortedDynamicWeights) {
			
			DocMetaWeight meta = (DocMetaWeight) metaO;
			wtFreshness = this.scoreFreshness(meta, ctx);
			if ( 0 != ipHouse )wtIpProxim = this.scoreIpProximity(meta, ipHouse, ctx);
			wtSocial = this.scoreSocialText(meta, plan, ctx);
			wtTags = this.scoreTags(meta, plan, ctx);
			
			if ( DEBUG_ENABLED ) {
				if ( logTop10 < 10) {
					logTop10++;
					log.append(meta.serialId).append(">").append(meta.termWeight).
					append("|").append(wtFreshness).append("|").append(wtIpProxim).
					append("|").append(wtSocial).append("|").append(wtTags).append("|").
					append(meta.weight).append('\n');
				}
			}
			
			finalWeight = meta.weight * ctx.boostDocumentWeight;
			finalWeight = finalWeight + meta.termWeight; // Ternm weight is already boosted during static ranking
			finalWeight = finalWeight + wtFreshness;
			finalWeight = finalWeight +  wtIpProxim;
			finalWeight = finalWeight + wtSocial;
			finalWeight = finalWeight + wtTags;
			meta.weight = (int) (finalWeight * 100);
		}
		
		if ( DEBUG_ENABLED ) {
			OutpipeLog.l.debug("ComputeDynamicRanking > " + log.toString());
		}
		DocMetaWeight.sort(result.sortedDynamicWeights);
	}
	
	private int scoreFreshness(DocMeta meta, QueryContext ctx) {
		Date referenceDate = meta.modifiedOn;
		if ( null == referenceDate ) {
			referenceDate = meta.createdOn;
		}
		if ( null == referenceDate ) return 0;

		double totalScore = System.currentTimeMillis() - referenceDate.getTime();
		totalScore = 100 - (totalScore / 1170000000L);
		int score = new Double(totalScore).intValue();
		if ( score < 0 ) score = 0;
		return (score * ctx.boostFreshness);
	}
	
	private int scoreIpProximity(DocMeta meta,  int ipHouse, QueryContext ctx) {
		int ipScore = meta.ipHouse - ipHouse;
		ipScore = (ipScore < 0) ? ipScore * -1 : ipScore;
		if ( ipScore == 0) {
			ipScore = 1; //Complete Match
		} else {
			ipScore = new Double(100 - (Math.log10(ipScore) / 9 * 100)).intValue();
			ipScore = (ipScore < 0) ? ipScore * -1 : ipScore;
		}
		if ( 0 != ipScore) ipScore = ipScore/3;
		return ( ipScore * ctx.boostIpProximity); 
	}
	

	/**
	 * For each social text found in matching to term work
	 * 1 point is contributed for ranking. 
	 * @param meta
	 * @param planner
	 * @return
	 */
	private int scoreSocialText(DocMeta meta, QueryPlanner planner, QueryContext ctx) {
		if (null == meta.socialText) return 0;
		int socialRanking = 0;
		if ( null != planner.mustTerms) {
			for (QueryTerm term : planner.mustTerms) {
				if ( meta.socialText.indexOf(term.wordOrigLower) >= 0 ) {
					socialRanking++;
				}
			}
		}
		
		if ( null != planner.optionalTerms) {
			for (QueryTerm term : planner.optionalTerms) {
				if ( meta.socialText.indexOf(term.wordOrigLower) >= 0 ) {
					socialRanking++;
				}
			}
		}
		
		return ( socialRanking * ctx.boostChoices); 
	}
	
	/**
	 * For each term word found in social text which matches the provided tag 
	 * word, 1 point is contributed for ranking. 
	 * @param meta
	 * @param planner
	 * @return
	 */
	private int scoreTags(DocMeta meta, QueryPlanner planner, QueryContext ctx) {
		if (null == meta.tags) return 0;
		int tagRanking = 0;
		if ( null != planner.mustTerms) {
			for (QueryTerm term : planner.mustTerms) {
				if ( meta.tags.indexOf(term.wordOrigLower) >= 0 ) {
					tagRanking++;
				}
			}
		}
		
		if ( null != planner.optionalTerms) {
			for (QueryTerm term : planner.optionalTerms) {
				if ( meta.tags.indexOf(term.wordOrigLower) >= 0 ) {
					tagRanking++;
				}
			}
		}
		
		return tagRanking;
	}

	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}

	public PipeOut getInstance() {
		return this;
	}


	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}
	
	public String getName() {
		return "ComputeDynamicRanking";
	}		
}
