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

import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

/**
 * Persist to dictionary
 * @author karan
 *
 */
public class SaveToBufferedDictionary implements PipeIn {

	static Hashtable<String, DictEntry> entries = null;
	private static int cacheLimit = 20000;
	
	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;
		if ( null == doc.terms) return;
		
		List<Term> terms = doc.terms.all;
		if ( null == terms) return;
		
		/**
		 * Remove any duplicates from the local set
		 * Multi types are taken care
		 */
		Map<String, DictEntry> localSet = 
			ObjectFactory.getInstance().getDictEntryHash();
		
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
		if ( null == entries) entries = new Hashtable<String, DictEntry>();
		synchronized (entries) {
			for (String word : localSet.keySet()) {
				if ( entries.containsKey(word)) {
					entries.get(word).frequency++;
				} else entries.put(word, localSet.get(word));
			}
		}
		
		ObjectFactory.getInstance().putDictEntryHash(localSet);
	}

	/**
	 * Aggregate and commit all the records at one shot.
	 */
	public void commit(boolean multiwriter) throws ApplicationFault, SystemFault {
		if ( null == entries) return;
		if ( entries.size() > cacheLimit) flush(multiwriter);
	}
	
	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		int cacheSize = conf.getInt("dictionary.cache.size", 20000);
		if ( cacheLimit != cacheSize ) cacheLimit = cacheSize;  
	}

	public PipeIn getInstance() {
		return new SaveToBufferedDictionary();
	}

	public String getName() {
		return "SaveToBufferedDictionary";
	}
	
	public static void flush(boolean multiwriter) throws SystemFault, ApplicationFault {
		if ( null == entries) return;
		if ( 0 == entries.size()) return;

		SaveToDictionary dict = new SaveToDictionary();
		dict.entries = ObjectFactory.getInstance().getDictEntryHash();
		int i = 0;
		synchronized (entries) { //Flush in batch
			for ( String word : entries.keySet()){
				dict.entries.put(word, entries.get(word));
				i++;
				if ( i > 100) {
					dict.commit(multiwriter);
					dict.entries.clear();
					i = 0;
				}
			}
			entries.clear();
		}
		if ( i > 0) dict.commit(multiwriter); //Which is left in the batch mode
		ObjectFactory.getInstance().putDictEntryHash(dict.entries);
	}
}