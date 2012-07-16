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
package com.bizosys.hsearch.query;

import java.util.List;

import com.bizosys.hsearch.util.ObjectFactory;

/**
 * Best on term preciousness, optional and must conditions
 * the execution is sequenced. In each step one or more words
 * gets matched. Precious must words executed first. In all 
 * downlevel processing, proper filteration is applied to eliminate
 * unnecessary volume.
 * @author karan
 *
 */
public class QueryPlanner {

	public List<QueryTerm> mustTerms = null;
	public List<QueryTerm> optionalTerms = null;
	public List<List<QueryTerm>> sequences = null;
	
	public List<QueryTerm> phrases  = null;	
	
	public QueryPlanner() {
		
	}
	
	public void addPhrase(QueryTerm phrase) {
		if ( null == phrases) phrases = ObjectFactory.getInstance().getQueryTermsList();
		phrases.add(phrase);
	}
	
	public void addMustTerm(QueryTerm aTerm) {
		aTerm.isOptional = false;
		if ( null == mustTerms) mustTerms = ObjectFactory.getInstance().getQueryTermsList();
		for (QueryTerm term : mustTerms) {
			if ( term.wordStemmed.equals(aTerm.wordStemmed)) return;
		}
		mustTerms.add(aTerm);
		aTerm.isOptional = false;
	}
	
	public void addOptionalTerm(QueryTerm aTerm) {
		aTerm.isOptional = true;
		if ( null == optionalTerms) optionalTerms = ObjectFactory.getInstance().getQueryTermsList();
		
		for (QueryTerm term : optionalTerms) {
			if ( term.wordStemmed.equals(aTerm.wordStemmed)) return;
		}
		optionalTerms.add(aTerm);
		aTerm.isOptional = true;
	}
	
	public void cleanup() {
		if ( null != mustTerms) 
			ObjectFactory.getInstance().putQueryTermsList(mustTerms);
		if ( null != optionalTerms) 
			ObjectFactory.getInstance().putQueryTermsList(optionalTerms);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('\n');
		if ( null != mustTerms  ) {
			for (QueryTerm term : mustTerms) {
				sb.append("Must Term = ").append(term);
			}
		}
		if ( null != optionalTerms ) {
			for (QueryTerm term : optionalTerms) {
				sb.append("Optional Term = ").append(term);
			}
		}
		return sb.toString();
	}
	
}