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

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.IScanCallBack;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.RecordScalar;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

/**
 * Dictionary has around 1 Second. This should be taken care by 
 * batching this. BatchProcessor should do this one by one.
 * We should also perform mass updates. 
 * @author karan
 *
 */
public class Dictionary {
	
	/**
	 * Character separating Multiple keywords
	 */
	private static final char KEYWORD_SEPARATOR = '\t';
	
	/**
	 * Many words forming a single line. Stacking many words 
	 * in a line helps saving the storage space for fuzzy and regex queries
	 */
	int termMergeFactor = 1000;
	
	/**
	 * On retrieving dictionary, number of words per page
	 */
	int pageSize = 1000;

	/**
	 * The whole dictionary is stored as multiple lines and 
	 * in each line multiple words. This enables faster pattern 
	 * and fuzzy matching. 
	 */
	List<String> mergedWordLines = new ArrayList<String>(100);
	
	boolean threadSafe = false;
	
	private boolean isDebugEnabled = DictLog.l.isDebugEnabled();
	
	private String wordPrefix = StringUtils.Empty; 
	
	protected long touchTime = System.currentTimeMillis();
	public String tenant = StringUtils.Empty;
	
	/**
	 * Constructor
	 * @param termMergeFactor	Many words forming a single line.
	 * @param pageSize	On retrieving dictionary, number of words per page
	 * @param threadSafe Enable thread Safety.	
	 */
	public Dictionary(String tenant, int termMergeFactor, int pageSize, boolean threadSafe)
	throws ApplicationFault {
		
		if ( StringUtils.isEmpty(tenant)) throw new ApplicationFault("No tenant");
		this.tenant = tenant;
		
		this.wordPrefix = tenant + "/" ;
		this.termMergeFactor = termMergeFactor;
		this.pageSize = pageSize;
		this.threadSafe = threadSafe;
		this.touchTime = System.currentTimeMillis();
	}
	
	/**
	 * Add entries to the dictionary
	 * @param keywords	Dictionary words
	 * @throws SystemFault	Error
	 */
	public void add(Map<String, DictEntry> keywords) throws SystemFault {

		if ( null == keywords) return;
		
		if (isDebugEnabled) DictLog.l.debug( "Dictionary> Adding Keywords :" + keywords.size());
		
		List<RecordScalar> records = null;
		try {

			records = ObjectFactory.getInstance().getScalarRecordList();
			
			for (DictEntry entry : keywords.values()) {
				if (isDebugEnabled) DictLog.l.debug("Dictionary> Word = " + entry.word);
				if ( null == entry) continue;
				if ( null == entry.word) continue;
				Storable pk = new Storable(wordPrefix + entry.word);

				DictEntryMerge scalar = new DictEntryMerge( pk, 
					IOConstants.DICTIONARY_BYTES, IOConstants.DICTIONARY_TERM_BYTES, entry);
				records.add(scalar);
			}
			HWriter.getInstance(threadSafe).mergeScalar(IOConstants.TABLE_DICTIONARY, records);
		} catch (Exception ex) {
			DictLog.l.error(ex);
			throw new SystemFault(ex);
		} finally {
			if ( null != records) 
				ObjectFactory.getInstance().putScalarRecordList(records);
			this.touchTime = System.currentTimeMillis();
		}
	}
	
	/**
	 * Find exact entry detail from the dictionary.
	 * @param keyword	Word to be searched
	 * @return	The Dictionary Entry for the word
	 * @throws SystemFault	Error condition
	 */
	public DictEntry get(String keyword) throws SystemFault {
		if ( StringUtils.isEmpty(keyword) ) return null;
		if (isDebugEnabled) DictLog.l.debug("Dictionary> Getting Keyword :" + keyword);
		try {
			NV kv = new NV(IOConstants.DICTIONARY_BYTES, IOConstants.DICTIONARY_TERM_BYTES);
			Storable pk = new Storable(wordPrefix + keyword);
			RecordScalar scalar = new RecordScalar(pk,kv);
			HReader.getScalar(IOConstants.TABLE_DICTIONARY, scalar);
			if ( null == scalar.kv.data) return null;
			return new DictEntry(scalar.kv.data.toBytes());
		} catch (Exception ex) {
			throw new SystemFault("Error in dictionary resolution for :" + keyword, ex);
		} finally {
			this.touchTime = System.currentTimeMillis();
		}
	}
	
	/**
	 * Stream all values.
	 * @param writer
	 * @throws SystemFault
	 */
	public void getAll(String indexLetters, Writer writer) throws SystemFault {
		IScanCallBack callBack = new StreamDictionaryEntries(writer);

		NV nv = new NV(IOConstants.DICTIONARY_BYTES, IOConstants.DICTIONARY_TERM_BYTES);
		HReader.getAllValues(IOConstants.TABLE_DICTIONARY, nv, indexLetters, callBack);
		this.touchTime = System.currentTimeMillis();
	}
	
	
	/**
	 * Builds the dictionary terms for regex and fuzzy searches  
	 * @throws SystemFault	Storage Failure
	 */
	public synchronized void buildTerms() throws SystemFault {
		DictLog.l.info("Dictionary> Term building START");
		
		DictionaryBook book = new DictionaryBook(termMergeFactor, KEYWORD_SEPARATOR, this.wordPrefix);

		NV kv = new NV(IOConstants.DICTIONARY_BYTES, IOConstants.DICTIONARY_TERM_BYTES);
		HReader.getAllKeys(IOConstants.TABLE_DICTIONARY, kv, this.wordPrefix, book);
		
		/**
		 * Swap the temp with merged one
		 * TODO:// This is not memory efficient with growing number of words
		 * Think of finding and deleting from the stack
		 */
		List<String> cleanThis = this.mergedWordLines;
		this.mergedWordLines = book.getLines();
		
		cleanThis.clear();
		cleanThis = null;
		
		DictLog.l.info("Dictionary> Term building END");
		this.touchTime = System.currentTimeMillis();
	}
	
	/**
	 * Uses fuzzy mechanism for searching.
	 * @param searchWord	Fuzzy word to be scanned
	 * @param fuzzyFactor	Low fuzzy means accurate matching. 
	 * A value of 3 is a good fuzzy matching for named. 
	 * @return	Matching words
	 */
	public List<String> fuzzy(String searchWord, int fuzzyFactor) {
		DistanceImpl dis = new DistanceImpl();
		List<String> foundWords = new ArrayList<String>();
		int index1, index2;
		String token = null;
		
		for (String text: mergedWordLines) {
			index1 = 0;
			index2 = text.indexOf(KEYWORD_SEPARATOR);
			token = null;
			while (index2 >= 0) {
				token = text.substring(index1, index2);
				index1 = index2 + 1;
				index2 = text.indexOf(KEYWORD_SEPARATOR, index1);
				if ( StringUtils.isEmpty(token) ) continue;

				if ( dis.getDistance(searchWord, token) <= fuzzyFactor) {
					foundWords.add(token);
				}
			}
		}
		this.touchTime = System.currentTimeMillis();
		return foundWords;
	}
	
	/**
	 * Uses regular expression to find it.
	 * @param pattern	The regex pattern for the word
	 * @return	List of matching words
	 */
	public synchronized List<String> regex(String pattern) {
		Pattern p = Pattern.compile(pattern);
		List<String> matchedWords = new ArrayList<String>();
		
		int readIndex, foundIndex;
		String token = null;
		Matcher m = null;
		for (String text: mergedWordLines) {
			readIndex = 0;
			  
			foundIndex = text.indexOf(KEYWORD_SEPARATOR);
			if ( foundIndex == -1 && text.length() > 0) {
				m = p.matcher(text);
				if ( m.find() ) matchedWords.add(text);
			}
			  
			token = null;
			while (foundIndex >= 0) {
				token = text.substring(readIndex, foundIndex);
				m = p.matcher(token);
				if ( m.find() ) matchedWords.add(token);
				readIndex = foundIndex + 1;
				foundIndex = text.indexOf(KEYWORD_SEPARATOR, readIndex);
			}
		}
		this.touchTime = System.currentTimeMillis();
		return matchedWords;
	}
	
	/**
	 * Delete the occurance of supplied words from dictionary
	 * @param keywords	The words to be deleted
	 * @throws SystemFault
	 */
	public void delete(Collection<String> keywords) throws SystemFault {
		if ( null == keywords) return;
		List<byte[]> deletes = null;
		try {
			deletes = ObjectFactory.getInstance().getByteArrList();
			for (String keyword : keywords) {
				if ( StringUtils.isEmpty(keyword)) continue;
				byte[] pk = Storable.putString(wordPrefix + keyword);
				deletes.add(pk);
				continue;
			}
			HDML.truncateBatch(IOConstants.TABLE_DICTIONARY, deletes);
		} catch (Exception ex) {
			DictLog.l.error(ex);
			throw new SystemFault(ex);
		} finally {
			if ( null != deletes) ObjectFactory.getInstance().putByteArrList(deletes);
			this.touchTime = System.currentTimeMillis();
		}
	}
	
	public void delete(String keyword) throws SystemFault {
		if ( StringUtils.isEmpty(keyword) ) return;
		try {
			Storable pk = new Storable(wordPrefix + keyword);
			HWriter.getInstance(threadSafe).delete(IOConstants.TABLE_DICTIONARY, pk);
		} catch (Exception ex) {
			DictLog.l.error(ex);
			throw new SystemFault(ex);
		} finally {
			this.touchTime = System.currentTimeMillis();
		}
	}		

	/**
	 * Lower the sighting frequencies of the dictionary entries
	 * @param keywords	"Keyword-Dictionary Entry" map 
	 * @throws SystemFault	Error condition
	 */
	public void substract(Map<String, DictEntry> keywords) throws SystemFault {
		
		if ( null == keywords) return;
		
		List<RecordScalar> records = new ArrayList<RecordScalar>(keywords.size());
		for (DictEntry entry : keywords.values()) {
			if ( null == entry) continue;
			if ( null == entry.word) continue;

			DictEntrySubstract scalar = new DictEntrySubstract(
				new Storable(wordPrefix + entry.word), IOConstants.DICTIONARY_BYTES,
				IOConstants.DICTIONARY_TERM_BYTES, entry);
			records.add(scalar);
		}
		
		try {
			HWriter.getInstance(threadSafe).mergeScalar(IOConstants.TABLE_DICTIONARY, records);
		} catch (Exception ex) {
			DictLog.l.error(ex);
			throw new SystemFault(ex);
		} finally {
			this.touchTime = System.currentTimeMillis();
		}
	}
	
	public void clean() {
		if ( null != this.mergedWordLines) this.mergedWordLines.clear();
	}
}
