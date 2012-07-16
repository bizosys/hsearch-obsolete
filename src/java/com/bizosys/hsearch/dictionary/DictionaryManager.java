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

package com.bizosys.hsearch.dictionary;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.services.scheduler.ExpressionBuilder;
import com.bizosys.oneline.services.scheduler.ScheduleTask;
import com.bizosys.oneline.util.StringUtils;

/**
 * This is the Facade for the dictionry service. It's responsible for
 * initializing dictionry service as well as serving clients. This is the
 * single entry point for outside clients to perform dictionry operations.
 * @author karan
 *
 */
public class DictionaryManager implements Service{
	
	/**
	 * Dictionry Refresh Task. In a multi machine environment, this keeps 
	 * loading the dictionry to in memory locally in interval to ensure
	 * fuzzy and regex searches.
	 */
	ScheduleTask scheduledRefresh = null;
	
	/**
	 * The dictionry repositories for the tenants
	 * Once loaded, they are cached
	 * TODO: Do maintenance work at interval.
	 * TODO: Migrate these to a caching framework which works on a given fixed memory. 
	 */
	Map<String, Dictionary> dictRepos = new ConcurrentHashMap<String, Dictionary>();
	
	int mergeCount = 1000;
	
	int pageSize = 1000;
	
	boolean isSpellChecked = true;
	
	boolean concurrency = true;
	
	int maxCacheSize = 10;
	
	int fuzzyLevel = 2;
	
	/**
	 * Singleton
	 */
	private static DictionaryManager instance = null;
	
	/**
	 * Constructor is private. This ensures singleton
	 * @return	DictionaryManager
	 * @throws SystemFault
	 */
	public static final DictionaryManager getInstance() throws SystemFault {
		if ( null == instance) throw new SystemFault(
			"DisctionaryManager is not initialized");
		return instance;
	}
	
	/**
	 *	Default Constructor 
	 *  Needs to be initialized only once. Done by ServiceFacade.
	 */
	public DictionaryManager() {
		instance = this;
	}
	
	/**
	 * Service name - Dictionarymanager
	 */
	public String getName() {
		return "Dictionarymanager";
	}

	/**
	 * Launches dictionry refresh task and initializes the dictionry.
	 */
	public boolean init(Configuration conf, ServiceMetaData arg1) {
		DictLog.l.info("Initializing Dictionary Service");
		DictionaryRefresh refreshTask = new DictionaryRefresh();

		try {
			this.mergeCount = conf.getInt("dictionary.merge.words", 1000);
			this.pageSize = conf.getInt("dictionary.page.Size", 1000);
			this.concurrency = conf.getBoolean("dictionary.concurrency", true);
			this.isSpellChecked = conf.getBoolean("dictionary.spellcheck.enabled", true);
			this.maxCacheSize = conf.getInt("dictionary.cache.Size", 10);
			this.fuzzyLevel = conf.getInt("dictionary.fuzzy.level", 2);

			int refreshInteral = conf.getInt("dictionary.refresh", 30);
			ExpressionBuilder expr = new ExpressionBuilder();
			expr.setSecond(0, false);
			expr.setMinute(refreshInteral, true);
			
			long startTime = new Date().getTime() + refreshInteral * 60 * 1000 /** After 10 minutes */;
			scheduledRefresh = new ScheduleTask(refreshTask, expr.getExpression(), 
					new Date(startTime), new Date(Long.MAX_VALUE));
			DictLog.l.info("DisctionaryManager > Dictionry Refresh task is scheduled.");
			return true;

		} catch (Exception ex) {
			DictLog.l.fatal("DisctionaryManager >", ex);
			return false;
		}
	}

	public void process(Request arg0, Response arg1) {
	}
	
	protected Map<String, Dictionary> getCachedTenants() {
		return dictRepos;
	}

	/**
	 * Stop the refresh task
	 */
	public void stop() {
		if ( null != this.scheduledRefresh) 
			this.scheduledRefresh.endDate = new Date(System.currentTimeMillis());
	}
	
	/**
	 * Removes the complete dictionary.
	 * @throws SystemFault
	 */
	public void purge() throws SystemFault {
		try {
			NV kv = new NV(IOConstants.DICTIONARY_BYTES, IOConstants.DICTIONARY_TERM_BYTES);
			HDML.truncate(IOConstants.TABLE_DICTIONARY, kv);
		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}

	/**
	 * If cached, give from cache or load it from the database
	 * @param tenant
	 * @return
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public Dictionary getDictionary(String tenant) throws ApplicationFault, SystemFault {
		if ( this.dictRepos.containsKey(tenant)) return this.dictRepos.get(tenant);
		
		Dictionary aDict = new Dictionary(tenant, mergeCount, pageSize, concurrency);
		this.dictRepos.put(tenant, aDict);
		if ( isSpellChecked ) {
			DictLog.l.info("DisctionaryManager > Caching dictionary terms to memory.");
			aDict.buildTerms();
		}
		return aDict;
	}

	
	/**
	 * Get the first page words from the dictionary
	 * @return	List of words
	 * @throws SystemFault
	 */
	public void getKeywords(String tenant, Writer writer) 
	throws ApplicationFault, SystemFault {
		
		try {
			Dictionary aDict = getDictionary(tenant);
			writer.append("<words>");
			aDict.getAll(StringUtils.Empty, writer);
			writer.append("</words>");
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}
	
	/**
	 * Get the first page words from the dictionary
	 * @return	List of words
	 * @throws SystemFault
	 */
	public void getKeywords(String tenant, String indexLetter, Writer writer) 
	throws ApplicationFault, SystemFault {
		
		try {
			Dictionary aDict = getDictionary(tenant);
			writer.append("<words>");
			aDict.getAll(indexLetter, writer);
			writer.append("</words>");
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}	

	/**
	 * Add a single entry to the dictionry
	 * @param entry
	 * @throws SystemFault
	 */
	public void add(String tenant, DictEntry entry) throws ApplicationFault, SystemFault {
		Dictionary aDict = getDictionary(tenant);
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>(1);
		entries.put(entry.word, entry);
		aDict.add(entries);
	}

	/**
	 * Add bunch of entries to the dictionry
	 * @param entries
	 * @throws SystemFault
	 */
	public void add(String tenant, Map<String, DictEntry> entries) throws ApplicationFault, SystemFault {
		Dictionary aDict = getDictionary(tenant);
		aDict.add(entries);
	}
	
	public void refresh(String tenant)  throws ApplicationFault, SystemFault {
		Dictionary aDict = getDictionary(tenant);
		aDict.buildTerms();		
	}
	
	/**
	 * Get directly the keyword
	 * @param keyword
	 * @return	Dictionary Entry
	 * @throws SystemFault
	 */
	public DictEntry get(String tenant, String keyword) throws ApplicationFault,SystemFault {
		if ( StringUtils.isEmpty(keyword)) return null;

		Dictionary aDict = getDictionary(tenant);
		return aDict.get(keyword);
	}
	
	/**
	 * Check for the right spelling for the given keyword
	 * @param keyword
	 * @return	List of matching words
	 * @throws SystemFault
	 */
	public List<String> getSpelled(String tenant,String keyword) throws ApplicationFault,SystemFault {
		if ( StringUtils.isEmpty(keyword)) return null;

		Dictionary aDict = getDictionary(tenant);
		List<String> words = aDict.fuzzy(keyword, 1);
		if ( words.size() > 0) return words;

		return aDict.fuzzy(keyword, this.fuzzyLevel);
	}
	
	/**
	 * Gets matching keywords for the given wildcard keyword.
	 * @param keyword	The regular expression
	 * @return	List of matching words
	 * @throws SystemFault
	 */
	public List<String> getWildCard(String tenant, String keyword) throws ApplicationFault,SystemFault {
		if ( StringUtils.isEmpty(keyword)) return null;

		Dictionary aDict = getDictionary(tenant);
		return aDict.regex(keyword);
	}

	/**
	 * This completely removes the keywords from the dictionry
	 * @param keywords
	 * @throws SystemFault
	 */
	public void delete(String tenant, Collection<String> keywords) throws ApplicationFault,SystemFault {
		if ( null == keywords) return;
		Dictionary aDict = getDictionary(tenant);
		aDict.delete(keywords);
	}
	
	/**
	 * Delete a keyword from the dictionary
	 * @param keyword	The dictionary word
	 * @throws SystemFault	System Error
	 */
	public void delete(String tenant, String keyword) throws ApplicationFault,SystemFault {
		if ( StringUtils.isEmpty(keyword)) return;

		Dictionary aDict = getDictionary(tenant);
		aDict.delete(keyword);
	}	

	/**
	 * Once a document is removed, substract it's keywords from the dictionry
	 * If there are more 
	 * @param entries
	 * @throws SystemFault
	 */
	public void substract(String tenant, Map<String, DictEntry> entries) throws ApplicationFault,SystemFault {
		if ( null == entries) return;
		Dictionary aDict = getDictionary(tenant);
		aDict.substract(entries);
	}
}
