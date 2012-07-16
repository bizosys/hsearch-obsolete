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

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.util.ObjectFactory;

/**
 * Delete terms from dictionary
 * @author karan
 *
 */
public class DeleteFromDictionary implements PipeIn {

	Map<String, DictEntry> entries = null;
	String tenant = StringUtils.Empty;
	
	public void visit(Object objDoc, boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == objDoc) throw new ApplicationFault("No document");
		Doc doc = (Doc) objDoc;
		if ( null == doc.terms) return;
		this.tenant = doc.tenant;
		
		List<Term> terms = doc.terms.all;
		if ( null == terms) return;
		
		Map<String, DictEntry> localSet = 
			new Hashtable<String, DictEntry>(terms.size());
		for (Term term : terms) {
			if (StringUtils.isEmpty(term.termType)) continue;
			
			DictEntry de = null;
			if (localSet.containsKey(term.term)) {
				de = localSet.get(term.term);
				de.addType(term.termType);
			} else {
				de = new DictEntry(term.term, term.termType,1);
			}
			localSet.put(term.term, de);
		}
		
		/**
		 * Add the local set to merged one
		 */
		if ( null == entries) entries = ObjectFactory.getInstance().getDictEntryHash();
		for (String word : localSet.keySet()) {
			if ( entries.containsKey(word)) {
				entries.get(word).frequency++;
			} else entries.put(word, localSet.get(word));
		}
	}

	/**
	 * Aggregate and commit all the records at one shot.
	 */
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == entries) return;
		DictionaryManager s = DictionaryManager.getInstance();
		s.substract(tenant, entries);
		ObjectFactory.getInstance().putDictEntryHash(entries);
	}

	public void init(Configuration conf){
	}

	public PipeIn getInstance() {
		return new DeleteFromDictionary();
	}

	public String getName() {
		return "DeleteFromDictionary";
	}

}
