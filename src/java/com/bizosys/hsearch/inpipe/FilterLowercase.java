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

import java.util.List;
import java.util.Locale;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.util.LuceneConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Convert the text to lower case.
 * @author karan
 *
 */
public class FilterLowercase implements PipeIn {
	
	public FilterLowercase() {}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterLowercase";
	}

	public void init(Configuration conf) {
	}

	public void visit(Object docObj, boolean concurrency) throws ApplicationFault {
		
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;
		DocTerms terms = doc.terms;
		if ( null == terms) throw new ApplicationFault("No Terms");
		
		if ( null != doc.meta) {
			if ( null == doc.meta.locale)
				doc.meta.locale = Locale.ENGLISH;
			/**
			if ( ! Locale.ENGLISH.getLanguage().equals(
					doc.meta.locale.getLanguage()) ) {
				
				InpipeLog.l.warn( Locale.ENGLISH.getLanguage() 
					+ "/" + doc.meta.locale.getLanguage()); 
				throw new ApplicationFault("Not English");
			}
			*/
		}

		
		List<TermStream> streams = terms.getTokenStreams();
		if ( null == streams) return; //Allow for no bodies
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new LowerCaseFilter(LuceneConstants.version, stream);
			ts.stream = stream;
		}
		return;
	}

	public void commit(boolean arg0) throws ApplicationFault, SystemFault {
	}
}