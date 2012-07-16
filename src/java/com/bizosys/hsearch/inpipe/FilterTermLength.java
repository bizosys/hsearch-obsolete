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

import org.apache.lucene.analysis.LengthFilter;
import org.apache.lucene.analysis.TokenStream;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Any default any word less than 2 character or more than 200 character
 * are eliminated.  To change this behavior, <code>word.cutoff.minimum</code> and 
 * <code>word.cutoff.maximum</code> parameters can be adjusted from the <code>site.xml</code>
 * configuration file.
 * @author karan
 *
 */
public class FilterTermLength implements PipeIn {
	
	public int minCharCutoff = 2;
	public int maxCharCutoff = 200;
	
	public FilterTermLength() {}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterTermLength";
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.minCharCutoff = conf.getInt("word.cutoff.minimum", 2);
		this.maxCharCutoff = conf.getInt("word.cutoff.maximum", 200);
	}

	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;
		DocTerms terms = doc.terms;
		if ( null == terms)  throw new ApplicationFault("No Terms");
		
		List<TermStream> streams = terms.getTokenStreams();
		if ( null == streams) return; //Allow for no bodies
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) continue;
			stream = new LengthFilter(true, stream, minCharCutoff, maxCharCutoff);
			ts.stream = stream;
		}
	}
	
	public void commit(boolean arg0) throws ApplicationFault, SystemFault {
	}
}
