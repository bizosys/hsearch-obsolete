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

import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;

import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

/**
 * Consult the <code>DictionaryManager</code> to find details 
 * about the word.
 * @see DictionaryManager
 * @author karan
 *
 */
public class DictionaryEnrichment implements PipeOut{
	
	public DictionaryEnrichment() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		HQuery query = (HQuery) objQuery;
		QueryPlanner planner = query.planner;
		String tenant = query.ctx.getTenant();
		
		loadFromDictionary(tenant, planner.mustTerms);
		loadFromDictionary(tenant, planner.optionalTerms);
	}

	private void loadFromDictionary(String tenant, List<QueryTerm> queryWordL) 
	throws ApplicationFault, SystemFault {
		
		if ( null == queryWordL) return;
		for (QueryTerm term : queryWordL) {
			if ( OutpipeLog.l.isDebugEnabled() ) OutpipeLog.l.debug(
				"DictionaryEnrichment > " + term.wordStemmed);
			DictEntry entry = DictionaryManager.getInstance().get(tenant, term.wordStemmed);
			if( null == entry) continue;
			if ( OutpipeLog.l.isDebugEnabled() ) OutpipeLog.l.debug("DictionaryEnrichment:" + entry.toString() );
			term.foundTerm = entry;
		}
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
	}

	public PipeOut getInstance() {
		return this;
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}
	
	public String getName() {
		return "DictionaryEnrichment";
	}
	
}
