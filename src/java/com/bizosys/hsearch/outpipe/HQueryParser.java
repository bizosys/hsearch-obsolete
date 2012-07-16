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

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.hsearch.query.ReserveQueryWord;
import com.bizosys.hsearch.util.LuceneConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;
import com.bizosys.oneline.util.StringUtils;

/**
 * Internally it uses LuceneQueryParser for parsing the query.
 * Once parsed, based on the reserve words, it builds the 
 * query execution plan.
 * @author karan
 *
 */
public class HQueryParser implements PipeOut{
	
	public HQueryParser() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {

		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		if ( null == ctx || null == ctx.queryString) {
			throw new ApplicationFault("Blank Query "); 
		}

		if ( OutpipeLog.l.isDebugEnabled() )
			OutpipeLog.l.debug("Query String = " + ctx.queryString);

		parse(ctx.queryString, planner,ctx);
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}

	public PipeOut getInstance() {
		return this;
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}
	
	public String getName() {
		return "HQueryParser";
	}
	
	
	private static void parse(String text, 
		QueryPlanner planner, QueryContext hq) throws ApplicationFault, SystemFault {
		
		text = text.toLowerCase();
		List<Section> splits = quotedText(text, '"');
		List<String> words = tokenize(text, splits);
		
		List<QueryTerm> lastTerms = new ArrayList<QueryTerm>(3);
		
		Iterator<String> itr = words.iterator();
		boolean optionalMode = true;
		boolean isNot = false;
		Set<String> stopwords = StopwordManager.getInstance().getStopwords();
		while (itr.hasNext()) {
			String word = itr.next();
			word = word.trim();
			if ( word.length() == 0 ) continue;
			
			/**
			 * Don't proceed if this is a stopword
			 */
			
			if ( "and".equals(word) ) {
				if ( 0 != lastTerms.size() && null != planner.optionalTerms) {
					for (QueryTerm lastTerm : lastTerms) {
						if ( planner.optionalTerms.contains(lastTerm) ) {
							planner.optionalTerms.remove(lastTerm);
							planner.addMustTerm(lastTerm);
							OutpipeLog.l.trace(lastTerm);
						}
					}
				}
				optionalMode = false;
				
			} else if ( "or".equals(word) ) {
				if ( 0 != lastTerms.size() && null != planner.mustTerms) {
					for (QueryTerm lastTerm : lastTerms) {
						if ( planner.mustTerms.contains(lastTerm) ) {
							planner.mustTerms.remove(lastTerm);
							planner.addOptionalTerm(lastTerm);
						}
					}
				}
				optionalMode = true;
			} else if ( "not".equals(word) ) {
				isNot = true;
			} else {
				if ( stopwords.contains(word) ) continue;
				char firstChar = word.charAt(0);
				switch(firstChar) {
					case '+' :
						optionalMode = false;
						word = word.substring(1);
						break;
					case '-' :
						optionalMode = true;
						word = word.substring(1);
						break;
					case '!' :
						isNot = true;
						word = word.substring(1);
						break;
					default:
				}
				
				lastTerms.clear();
				
				//Make the base term
				QueryTerm reserveTerm =  new QueryTerm(word,isNot);
				int reserveWord = ReserveQueryWord.getInstance().
				mapReserveWord(reserveTerm.termType);		  
				if ( ReserveQueryWord.NO_RESERVE_WORD != reserveWord) {
					hq.populate(reserveWord, reserveTerm.wordOrig);
					lastTerms.add(reserveTerm);
				} else {
					planner.addPhrase(reserveTerm);
					List<String> lstWord = standardTokenizer(reserveTerm.wordOrig);
					for (String aWord : lstWord) {
						QueryTerm term =  new QueryTerm(aWord,isNot);
						term.setTermType(reserveTerm.termType);
						if (optionalMode) planner.addOptionalTerm(term);
						else planner.addMustTerm(term);
						hq.totalTerms++;
						lastTerms.add(term); //Make it a Array
					}
				}

				//Refresh the settings
				optionalMode = true;
				isNot = false;
			}
		}
		
		lastTerms.clear();
		lastTerms = null;
		
		if ( null != planner.optionalTerms && null == planner.mustTerms && 
			planner.optionalTerms.size() == 1 ) {
			planner.addMustTerm(planner.optionalTerms.get(0));
			planner.optionalTerms.clear();
			planner.optionalTerms = null;
		}

		if ( OutpipeLog.l.isDebugEnabled() ) {
			OutpipeLog.l.debug("Planner: " + planner.toString());
		}
	}

	private static List<String> standardTokenizer(String word) throws ApplicationFault {
		Reader reader = new StringReader(word);
		StandardTokenizer fil = new StandardTokenizer(LuceneConstants.version, reader);
		List<String> lstWord = new ArrayList<String>(3);
		try {
			word = null;
			CharTermAttribute termA = (CharTermAttribute)fil.getAttribute(CharTermAttribute.class);
			fil.reset();
			
			while ( fil.incrementToken()) {
				word = termA.toString();
				lstWord.add(word);
			}
			reader.close();
		} catch ( Exception ex) {
			throw new ApplicationFault(ex);
		}
		return lstWord;
	}

	private static List<String> tokenize(String text, List<Section> splits) {
		int lastIndex = 0;
		List<String> words = null;;
		int textLastIndex = text.length() - 1;
		String phrase = null;
		
		for (Section is : splits) {
			
			//Not a phrase start, Take the section
			if ( lastIndex != (is.start - 1)) {
				List<String> splittedWords = spaceTokenizer(
					text.substring(lastIndex, is.start - 1).trim(), ' ');
				if ( null != phrase ) {
					appendPhrase(phrase, splittedWords);
					phrase = null;
				}
				if ( null == words)words =  new ArrayList<String>(splits.size() * 2 + 1);
				words.addAll(splittedWords);
			}
			
			//Extract the phase
			phrase = text.substring(is.start, is.end );
			boolean isIsolatedStart = ( (is.start -1) == 0 || 
				( is.start > 1 && text.charAt(is.start - 2) == ' '));
			boolean isIsolatedEnd = (is.end == textLastIndex) ||
					(text.charAt(is.end + 1) == ' ');

			if ( isIsolatedStart && isIsolatedEnd ) {
				if ( null == words)words =  new ArrayList<String>(splits.size() * 2 + 1);
				words.add(phrase);
				phrase = null;
			} else if (isIsolatedEnd) {
				if ( null == words) {
					words =  new ArrayList<String>(splits.size() * 2 + 1);
					words.add(phrase);
				} else {
					int lst = words.size() - 1;
					String word = words.get(lst);
					words.remove(lst);
					words.add(word + phrase);
				}
				phrase = null;
			}
			
			lastIndex = is.end + 1;
		}
		
		//Remaining last section
		if  ( lastIndex <= textLastIndex) {
			List<String> splittedWords = StringUtils.fastSplit(
					text.substring(lastIndex), ' ');
			if ( null != phrase ) {
				appendPhrase(phrase, splittedWords);
				phrase = null;
			}
			if ( null == words)words =  new ArrayList<String>(splits.size() * 2 + 1);
			words.addAll(splittedWords);
		} else if (null != phrase) {
			if ( null == words) words =  new ArrayList<String>(splits.size() * 2 + 1);
			words.add(phrase);
			phrase = null;
		}
		return words;
	}
	
	private static void appendPhrase(String phrase, List<String> splittedWords) {
		if ( null != phrase) {
			if ( splittedWords.size() > 0) {
				phrase = phrase + splittedWords.get(0);
				splittedWords.remove(0);
				splittedWords.add(0,phrase);
			} else {
				splittedWords.add(phrase);
			}
			phrase = null;
		}
	}
	
	  public static List<Section> quotedText(final String text, char separator) {

		  final List<Section> result = new ArrayList<Section>();
		  int index1 = text.indexOf(separator);;
		  int index2 = 0; 
		  int lastIndex = text.length() - 1;

		  while (index1 >= 0) {
			  if ( (-1 == index1) || (index1 >= lastIndex) ) break;
			  index2 = text.indexOf(separator, index1 + 1);
			  if ( -1 == index2) break;
			  result.add(new Section(index1+1,index2));
			  index1 = text.indexOf(separator, index2 + 1);
		  }
		  return result;
	  }	
	  
	  public static List<String> spaceTokenizer(final String text, char separator) {
		  final List<String> result = new ArrayList<String>();
		  int index1 = 0;
		  int index2 = text.indexOf(separator);
		  String token = null;
		  if ( index2 == -1) {
			  result.add(text);
			  return result;
		  }
		  
		  while (index2 >= 0) {
			  token = text.substring(index1, index2);
			  result.add(token);
			  index1 = index2 + 1;
			  index2 = text.indexOf(separator, index1);
		  }
	            
		  if (index1 < text.length() - 1) {
			  result.add(text.substring(index1));
		  }
		  return result;
	  }	  
	  
	  public static class Section {
		  int start;
		  int end;
		  
		  public Section(int start, int end) {
			  this.start = start;
			  this.end = end;
		  }
		  
		  @Override
		  public String toString() {
			  return this.start + "-" + this.end;
		  }
	  }
}
