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

import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;

import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.outpipe.OutpipeLog;
import com.bizosys.hsearch.util.LuceneConstants;
import com.bizosys.oneline.SystemFault;

/**
 * Uses lucene query parsing classes to pass HSeach queries.
 * @author karan
 *
 */
public class LuceneQueryAnalyzer extends Analyzer {

	public LuceneQueryAnalyzer() {
	}
	
	public final TokenStream tokenStream(String fieldName, Reader reader) {
		
        Tokenizer tokenStream = new WhitespaceTokenizer(
        	LuceneConstants.version, reader);
        TokenStream result = new StandardFilter(
        	LuceneConstants.version, tokenStream);
        //result = new LengthFilter(result, 2, 200);
        
        try {
        	final Set<String> stopWords = StopwordManager.getInstance().getStopwords();
        	if ( null != stopWords) { 
           		result = new StopFilter(
           			LuceneConstants.version, result, stopWords);
        	}
        } catch (SystemFault ex) {
        	OutpipeLog.l.fatal("LuceneQueryAnalyzer > tokensteam" , ex);
        }
        return result;
    }
}
