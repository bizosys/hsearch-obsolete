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
package com.bizosys.hsearch.inpipe.util;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.StopFilter;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.services.batch.BatchTask;
import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.dictionary.DictLog;
import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.inpipe.InpipeLog;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.LuceneConstants;
import com.bizosys.hsearch.util.RecordScalar;

/**
 * Scheduler stopword refresh task
 * @author karan
 *
 */
public class StopwordRefresh implements BatchTask {
	
	private static final char STOPWORD_SEPARATOR = '\t';
	private static byte[] STOP_WORD_LISTS_KEY = "STOP_WORDS".getBytes();

	public String getJobName() {
		return "StopWordRefresh";
	}

	public Object process() throws ApplicationFault, SystemFault {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES);
		RecordScalar scalar = new RecordScalar(STOP_WORD_LISTS_KEY,nv);
		HReader.getScalar(IOConstants.TABLE_CONFIG, scalar);
		if ( null != scalar.kv.data) {
			String words = new String(scalar.kv.data.toBytes());
			List<String> wordLst = StringUtils.fastSplit(words, STOPWORD_SEPARATOR);
			if (DictLog.l.isInfoEnabled() ) {
				DictLog.l.info("StopwordRefresh task is refreshing stopword lists");
			}
			StopwordManager.getInstance().stopWords = buildStopwords(wordLst);
		}
		return null;
	}

	public void setJobName(String arg0) {
	}
	
	/**
	 * This refreshes the stopword list.
	 * @param allStopWords
	 * @return
	 * @throws ApplicationFault
	 */
	@SuppressWarnings("unchecked")
	private Set<String> buildStopwords(List<String> allStopWords) throws SystemFault{
		
		if ( null == allStopWords) {
			InpipeLog.l.warn(" FilterStopWords: No stop words." );
			return null;
		}
		
		try {
			Set wordSet = StopFilter.makeStopSet(LuceneConstants.version, allStopWords);
			if (InpipeLog.l.isInfoEnabled()) { 
				InpipeLog.l.info(" StopwordManager: stopWords.size - " + wordSet.size());
			}
			return (Set<String>) wordSet;

		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}
	
	public static void add(List<String> lstWord, boolean concurrency) throws SystemFault{
		if ( null == lstWord) return;
		IStorable wordB = new Storable(
			StringUtils.listToString(lstWord, STOPWORD_SEPARATOR));
		NV nv = new NV(
			IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES,wordB);
		RecordScalar scalar = new RecordScalar(STOP_WORD_LISTS_KEY,nv);
		try {
			HWriter.getInstance(concurrency).insertScalar(IOConstants.TABLE_CONFIG, scalar);
		} catch (IOException ex) {
			InpipeLog.l.fatal("StopwordRefresh > ", ex);
			throw new SystemFault(ex);
		}
		
	}
}
