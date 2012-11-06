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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermList;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.lang.Stemmer;
import com.bizosys.hsearch.schema.EnglishMap;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * Extracted QueryTerm from the user search phrase.
 * @author karan
 *
 */
public class QueryTerm implements Comparator<QueryTerm> {
	
	public String wordOrig = null;
	public String wordOrigLower = null;
	public String wordStemmed = null;
	
	public boolean isNegation = true;
	public boolean isOptional = true;
	public boolean isTermType = false;
	public String termType = Term.NO_TERM_TYPE; //Age
	public Byte termTypeCode = TermType.NONE_TYPECODE;
	public Byte docTypeCode = DocumentType.NONE_TYPECODE; //Age

	public DictEntry foundTerm = null;
	public float preciousNess = 0.0f;
	
	public IMatch matcher = null;
	
	public ILanguageMap lang = new EnglishMap();
	public Map<Long, TermList> foundIds = new HashMap<Long, TermList>();

	public QueryTerm() {
	}
	
	public QueryTerm(String term) throws ApplicationFault {
		int divider = term.indexOf(':');
		if ( divider > 0) {
			this.termType = term.substring(0,divider);
			this.wordOrig = term.substring(divider+1);
			this.isTermType = true;			  
		} else this.wordOrig = term;
		
		this.wordOrigLower = this.wordOrig.toLowerCase();
		this.wordStemmed = Stemmer.getInstance().stem(this.wordOrigLower);
	}
	  
	  public QueryTerm(String term, boolean isNot) throws ApplicationFault {
		  this(term);
		  this.isNegation = isNot;
	  }
	  
	  /**
	   * Set a new term type for an existing no type.
	   * @param type
	   */
	  public void setTermType(String type) {
		  if ( StringUtils.isEmpty(type)) return;
		  if ( ! StringUtils.isEmpty(this.termType) ) return;
		  this.termType = type;
		  this.isTermType = true;			  
	  }
	
	public void setTermMatch(int matchingType) {
		switch (matchingType) {
			case  IMatch.ENDS_WITH:
				this.matcher = MatchEndsWith.getInstance();
				break;
			case  IMatch.EQUAL_TO:
				this.matcher =MatchEqualTo.getInstance();
				break;
			case  IMatch.GREATER_THAN:
				this.matcher = MatchGreaterThan.getInstance();
				break;
			case  IMatch.GREATER_THAN_EQUALTO:
				this.matcher = MatchGreaterThanEqualTo.getInstance();
				break;
			case  IMatch.LESS_THAN:
				this.matcher = MatchLessThan.getInstance();
				break;
			case  IMatch.LESS_THAN_EQUALTO:
				this.matcher = MatchLessThanEqualTo.getInstance();
				break;
			case  IMatch.PATTERN_MATCH:
				this.matcher = MatchPattern.getInstance();
				break;
			case  IMatch.RANGE:
				this.matcher = MatchRange.getInstance();
				break;
			case  IMatch.STARTS_WITH:
				this.matcher = MatchStartsWith.getInstance();
				break;
			case  IMatch.WITH_IN:
				this.matcher = MatchWithIn.getInstance();
				break;
			default:
				break;
		}
	}
	
	public int compare(QueryTerm term1, QueryTerm term2) {
		return (int) (term1.preciousNess * 100 - term2.preciousNess * 100);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if ( null != wordOrig)
			sb.append(" Word = " ).append(wordOrig).append(" :: ");
		if ( null != wordStemmed)
			sb.append(" Stemmed = " ).append(wordStemmed).append(" :: ");
		sb.append(" , Type: " ).append(termType);
		sb.append(" , Match: " ).append(matcher).append('\n');
		return sb.toString();
	}
}
