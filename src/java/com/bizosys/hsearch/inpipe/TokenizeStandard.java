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
package com.bizosys.hsearch.inpipe;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.inpipe.util.ReaderType;
import com.bizosys.hsearch.util.LuceneConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Standard Tokenizer
 * @author karan
 *
 */
public class TokenizeStandard extends TokenizeBase implements PipeIn {

	public TokenizeStandard() {
		super();
	}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "TokenizeStandard";
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}

	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;
		doc.readers = super.getReaders(doc);
    	if (null == doc) return;
		
		try {
    		Analyzer analyzer = new StandardAnalyzer(LuceneConstants.version);
	    	for (ReaderType reader : doc.readers) {
				if ( null == doc.terms) doc.terms = new DocTerms();
	    		TokenStream stream = analyzer.tokenStream(
	    				reader.type, reader.reader);
	    		TermStream ts = new TermStream(
		    			reader.docSection, stream, reader.type); 
		    	doc.terms.addTokenStream(ts);
	    		//reader.reader stream is used during token computing.
			}
	    	analyzer.close();
    	} catch (Exception ex) {
    		throw new SystemFault(ex);
    	}
	}

	public void commit(boolean arg0) throws ApplicationFault, SystemFault {
	}
}
