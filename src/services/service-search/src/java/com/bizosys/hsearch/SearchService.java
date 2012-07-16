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

package com.bizosys.hsearch;

import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.AccessDefn;
import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.BucketIsFullException;
import com.bizosys.hsearch.common.Field;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.SField;
import com.bizosys.hsearch.common.WhoAmI;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IndexReader;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.lang.Stemmer;
import com.bizosys.hsearch.loader.RowEventProcessor;
import com.bizosys.hsearch.loader.DataLoader;
import com.bizosys.hsearch.loader.RowEventProcessorHSearch;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.schema.SchemaManager;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.services.InvalidRequestException;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;
import com.bizosys.oneline.util.XmlUtils;

public class SearchService implements Service {

	public static Logger l = Logger.getLogger(SearchService.class.getName());
	
	Configuration conf = null;
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		this.conf = conf;
		try {
			l.info("Initializing Scheme.");
			SchemaManager.getInstance().init(conf, meta);
			l.info("Scheme initialized.");
			
			XmlUtils.xstream.alias("hdoc", HDocument.class);
			XmlUtils.xstream.alias("field", SField.class);
			XmlUtils.xstream.alias("aword", DictEntry.class);
			XmlUtils.xstream.alias("user", WhoAmI.class);
			XmlUtils.xstream.alias("access", AccessDefn.class);
			
			return true;
		} catch (Exception ex) {
			l.fatal("Search Service Initialization Failed.");
			return false;
		}
	}

	public void stop() {
	}
	
	public String getName() {
		return "SearchService";
	}	
	
	public void process(Request req, Response res) {
		l.info(">> Searchservice ENTER");
		String action = req.action; 

		try {
			AccountInfo account = Account.getActiveAccountInfo(req, res);
			if ( null == account) return;

			if ( "document.add".equals(action) ) {
				this.addDocument(req, res, account);

			}  else if ( "document.get".equals(action) ) {
				this.getDocuemnt(req, res, account);

			}  else if ( "document.addXml".equals(action) ) {
				this.addXmlDocument(req, res, account);
				
			}  else if ( "document.load".equals(action) ) {
				this.loadDocument(req, res, account);
				
			}  else if ( "document.batch".equals(action) ) {
				this.doBatchAdd(req, res, account);
				
			} else if ( "document.delete".equals(action) ) {
				deleteDocument(req, res, account);

			} else if ( "document.deletes".equals(action) ) {
				deleteDocuments(req, res, account);

			} else if ( "document.search".equals(action) ) {
				this.searchDocument(req, res, account);

			}  else if ( "dictionary.lookup".equals(action) ) {
				this.lookupDictionary(req,res, account);
				
			}  else if ( "dictionary.refresh".equals(action) ) {
				this.refreshDictionary(req,res, account);

			}  else if ( "dictionary.list".equals(action) ) {
				this.listDictionary(req,res, account);
				
			}  else if ( "dictionary.add".equals(action) ) {
				this.addDictionary(req,res, account);

			}  else if ( "dictionary.addAll".equals(action) ) {
				this.addBatchDictionary(req,res, account);
				
			}  else if ( "dictionary.spell".equals(action) ) {
				this.spell(req,res, account);

			}  else if ( "dictionary.regex".equals(action) ) {
				this.regex(req,res, account);

			}  else if ( "dictionary.delete".equals(action) ) {
				this.delete(req,res, account);

			}  else if ( "dictionary.delete-special".equals(action) ) {
				this.deleteSpecial(req,res, account);

			} else {
				res.error("Failed Unknown operation : " + action);
			}
		} catch (InvalidRequestException ix) {
			l.fatal("SearchService > ", ix);
			res.error( ix.getMessage());
		} catch (Exception ix) {
			l.fatal("SearchService > ", ix);
			res.error( ix.getMessage());
		}
	}
	
	/**
	 * Gets a document given the {id}
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	private void getDocuemnt(Request req, Response res, AccountInfo account) throws ApplicationFault, SystemFault{
		String docId = req.getString("id", true, true, false);
		Doc d = IndexReader.getInstance().get(account.name, docId);
		try {
			d.toXml(res.getWriter());
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}

	/**
	 * Indexes a String. For indexing along with File Upload
	 * happens through fileuploadservlet.xml
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 * @throws ParseException
	 */
	private void addDocument(Request req, Response res, AccountInfo acc) 
	throws SystemFault, ApplicationFault, BucketIsFullException {

		String hDocXml = req.getString("hdoc", true, true, false);
		HDocument hdoc = (HDocument)  
			XmlUtils.xstream.fromXML(StringEscapeUtils.unescapeXml(hDocXml));

		hdoc.tenant = acc.name;
		String runPlan = req.getString("runplan", false,true,true);
		
		if ( null != hdoc) {
			if ( StringUtils.isEmpty(hdoc.ipAddress)) {
				hdoc.ipAddress = req.clientIp;
			}
		}

		if ( StringUtils.isEmpty(runPlan) ) {
			IndexWriter.getInstance().insert(hdoc,acc,true);
		} else {
			IndexWriter.getInstance().insert(hdoc, acc,
				IndexWriter.getInstance().getPipes(runPlan),true);
		}
		res.writeXml("<id>OK</id>");
	}
	
	/**
	 */
	private void addXmlDocument(Request req, Response res, AccountInfo acc) 
	throws SystemFault, ApplicationFault, BucketIsFullException {

		String hDocXml = req.getString("hdoc", true, true, false);
		HDocument hdoc = (HDocument)  
			XmlUtils.xstream.fromXML(StringEscapeUtils.unescapeXml(hDocXml));

		String xmlDoc = req.getString("xmldoc", true, true, false);
		String titleFields = req.getString("title.fields", false, true, false);
		String runPlan = req.getString("runplan", false,true,true);
		String separator = req.getString("separator", false,true,true);
		boolean generateCacheText = req.getBoolean("textify", false);

		separator = parseSeparator(separator);
		
		if ( null != hdoc) {
			if ( StringUtils.isEmpty(hdoc.ipAddress)) {
				hdoc.ipAddress = req.clientIp;
			}
		}
			
		StringBuilder sb = null;
		if ( generateCacheText ) sb = new StringBuilder();

		XmlToFields xtf = new XmlToFields(separator);
		Map<String, String> flds = xtf.toMap(xmlDoc);

		if ( StringUtils.isEmpty(hdoc.docType)) hdoc.docType = xtf.recordType;
		boolean isFirst = true;
		
		if ( null != flds) {
			if ( null == hdoc.fields ) 
				hdoc.fields = new ArrayList<Field>(flds.size());
			
			for (String key : flds.keySet()) {
				Field fld = new SField(key, flds.get(key));
				hdoc.fields.add(fld);

				if (generateCacheText) {
					if (isFirst) isFirst = false;
					else sb.append(separator);
					sb.append(flds.get(key));
				}
			}
		}
		
		if ( generateCacheText ) hdoc.cacheText = sb.toString();
		
		if ( ! StringUtils.isEmpty(titleFields)) {
			StringBuilder titleSb = new StringBuilder();
			List<String> lstTitleFields = StringUtils.fastSplit(titleFields, ',');
			
			isFirst = true;
			for (String aFld : lstTitleFields) {
				String fldValue = flds.get(aFld);
				if ( StringUtils.isEmpty(fldValue)) continue;

				if ( isFirst ) isFirst = false;
				else titleSb.append(separator);
				
				titleSb.append(fldValue);
			}
		}
		
		
		if ( StringUtils.isEmpty(runPlan) ) {
			IndexWriter.getInstance().insert(hdoc,acc,true);
		} else {
			IndexWriter.getInstance().insert(hdoc, acc,
				IndexWriter.getInstance().getPipes(runPlan),true);
		}
		
		res.writeXml("<id>OK</id>");
	}	
	
	/**
	 * Loads a File
	 * @param req
	 * @param res
	 * @param acc
	 * @throws SystemFault
	 * @throws ApplicationFault
	 * @throws BucketIsFullException
	 */
	@SuppressWarnings("unchecked")
	private void loadDocument(Request req, Response res, AccountInfo acc) 
	throws SystemFault, ApplicationFault, BucketIsFullException {

		String hDocXml = req.getString("document.prestine", true, true, false);
		HDocument pristineDoc = (HDocument)  
			XmlUtils.xstream.fromXML(StringEscapeUtils.unescapeXml(hDocXml));

		pristineDoc.tenant = acc.name;
		String docUrlStr = req.getString("document.url", true, true, false);
		URL docUrl = null;
		try {
			docUrl = new URL(docUrlStr);
		} catch (MalformedURLException ex) {
			throw new ApplicationFault("Bad File Url : " + docUrlStr);
		}
		String docType = req.getString("document.type", true, true, false);
		
		String idPrefix = req.getString("id.prefix", false,true,true);
		if ( StringUtils.isEmpty(idPrefix) ) idPrefix = null;

		int idColumn = 
			(StringUtils.isEmpty(req.getString("id.column", false,true,true)) ) ?
				-1 : req.getInteger("id.column", false);
				
		String separator = req.getString("columns.separator", false,true,true);
		separator = parseSeparator(separator);

		String linebreak = req.getString("linebreak", false,true,true);

		String colFormats = req.getString("columns.format", true,true,false);
		String[] colFormatsA = StringUtils.getStrings(colFormats, ",");
		int colFormatsT = colFormatsA.length;
		int [] columnFormats = new int[colFormatsT];
		for ( int i=0; i<colFormatsT; i++ ) {
			columnFormats[i] = new Integer(colFormatsA[i]);
		}
		
		String nonEmptyCells = req.getString("columns.nonempty", false,true,true);
		String[] nonEmptyCellsA = StringUtils.getStrings(nonEmptyCells, ",");
		int nonEmptyCellsT = nonEmptyCellsA.length;
		int[] nonEmptyCellsI = new int[nonEmptyCellsT];
		for ( int i=0; i<nonEmptyCellsT; i++ ) {
			nonEmptyCellsI[i] = new Integer(nonEmptyCellsA[i]);
		}
		
		String titleCells = req.getString("columns.title", false,true,true);
		int[] titleCellsI = null;
		if ( StringUtils.isEmpty(titleCells)) {
			titleCellsI = new int[0];
		} else {
			String[] titleCellsA = StringUtils.getStrings(titleCells, ",");
			int titleCellsT = titleCellsA.length;
			titleCellsI = new int[titleCellsT];
			for ( int i=0; i<titleCellsT; i++ ) {
				titleCellsI[i] = new Integer(titleCellsA[i]);
			}
		}

		int keywordColumn = 
			(StringUtils.isEmpty(req.getString("keyword.column", false,true,true)) ) ?
				-1 : req.getInteger("keyword.column", false);
		
		int urlColumn = 
			(StringUtils.isEmpty(req.getString("url.column", false,true,true)) ) ?
				-1 : req.getInteger("url.column", false);

		int weightColumn = 
			(StringUtils.isEmpty(req.getString("weight.column", false,true,true)) ) ?
				-1 : req.getInteger("weight.column", false);

		String previewCells = req.getString("columns.preview", false,true,true);
		int[] previewCellsI = null;
		if ( ! StringUtils.isEmpty(previewCells)) {
			String[] previewCellsA = StringUtils.getStrings(previewCells, ",");
			int previewCellsT = previewCellsA.length;
			previewCellsI = new int[previewCellsT];
			for ( int i=0; i<previewCellsT; i++ ) {
				previewCellsI[i] = new Integer(previewCellsA[i]);
			}
		}
		
		String descCells = req.getString("columns.desc", false,true,true);
		int[] descCellsI = null;
		if ( ! StringUtils.isEmpty(descCells)) {
			String[] descCellsA = StringUtils.getStrings(descCells, ",");
			int descCellsT = descCellsA.length;
			descCellsI = new int[descCellsT];
			for ( int i=0; i<descCellsT; i++ ) {
				descCellsI[i] = new Integer(descCellsA[i]);
			}
		}
		
		
		String strColumnsAllowed = req.getString("columns.values.allowed", false,true,true);
		Object optionCheckO = null;
		if (! StringUtils.isEmpty(strColumnsAllowed)) {
			optionCheckO = req.getObject("columns.values.allowed", false);
		}
		Map<Integer, String[]> optionalCheck = ( null == optionCheckO ) ? null : 
			(Map<Integer, String[]>) optionCheckO;
		
		String strColumnsMax = req.getString("columns.values.max", false,true,true);
		Object maxCheckO = null;
		if (! StringUtils.isEmpty(strColumnsMax)) {
			maxCheckO = req.getObject("columns.values.max", false);
		}
		Map<Integer, Double> maxCheck = ( null == maxCheckO ) ? null : 
			(Map<Integer, Double>) maxCheckO;

		String strColumnsMin = req.getString("columns.values.min", false,true,true);
		Object minCheckO = null;
		if (! StringUtils.isEmpty(strColumnsMin)) {
			minCheckO = req.getObject("columns.values.min", false);
		}
		Map<Integer, Double> minCheck = ( null == minCheckO ) ? null : 
			(Map<Integer, Double>) minCheckO;

		String indexableCells = req.getString("columns.indexable", false,true,true);
		String[] indexableCellsA = StringUtils.getStrings(indexableCells, ",");
		int indexableCellsT = indexableCellsA.length;
		int[] indexableCellsI = new int[indexableCellsT];
		for ( int i=0; i<indexableCellsT; i++ ) {
			indexableCellsI[i] = new Integer(indexableCellsA[i]);
		}
		
		String runPlan = req.getString("index.runplan", false,true,true);
	    List<PipeIn> runSteps = ( StringUtils.isEmpty(runPlan) ) ? null : 
	    	IndexWriter.getInstance().getPipes(runPlan);
		int startIndex = req.getInteger("index.start", 0);
		Boolean xmlPreview = req.getBoolean("index.preview.xml", false);
		if ( null == xmlPreview) xmlPreview = true;
		
		int endIndex = (StringUtils.isEmpty(req.getString("index.end", false,true,true)) ) ? 
			-1 : req.getInteger("index.end", -1);
			
		
		int batchSize = (StringUtils.isEmpty(req.getString("index.batch.size", false,true,true)) ) ? 
			300 : req.getInteger("index.batch.size", 300);
		
		RowEventProcessor handler = new RowEventProcessorHSearch(
				acc, pristineDoc, runSteps,
				idPrefix, idColumn, urlColumn,weightColumn,
				titleCellsI,keywordColumn, previewCellsI, descCellsI,
				docType, indexableCellsI, 
				startIndex,endIndex,batchSize, xmlPreview, res.getWriter(), linebreak);
	    try {
			res.writeHeader();
		    DataLoader.load(docUrl, true, handler, separator, columnFormats, 
		    	nonEmptyCellsI, optionalCheck, minCheck, maxCheck);
	    } catch (Exception ex) {
	    	res.writeText("Error> " + StringUtils.stringifyException(ex));
	    } finally {
	    	res.writeFooter();
	    }
	    
	}

	/**
	 * Update a specific Field
	 * @param req
	 * @throws Exception
	 * @throws ApplicationFault
	 * @throws ParseException
	 * @throws ApplicationError
	 */
	@SuppressWarnings("unchecked")
	private void doBatchAdd(Request req, Response res, AccountInfo acc) 
	throws SystemFault, ApplicationFault, BucketIsFullException {
		
		String hDocsXml = req.getString("hdocs", true, true, false);
		List<HDocument> hdocs = (List<HDocument>) 
			XmlUtils.xstream.fromXML(StringEscapeUtils.unescapeXml(hDocsXml));

		String runPlan = req.getString("runplan", false,true,true);
		boolean concurrency = true;
		
		for (HDocument hdoc : hdocs) {
			if ( null == hdoc) continue;
			hdoc.tenant = acc.name;
			if ( StringUtils.isEmpty(hdoc.ipAddress)) {
				hdoc.ipAddress = req.clientIp;
			}
		}
		
		if ( StringUtils.isEmpty(runPlan) ) {
			IndexWriter.getInstance().insertBatch(hdocs, acc, concurrency);
		} else {
			IndexWriter.getInstance().insertBatch(hdocs, acc,
				IndexWriter.getInstance().getPipes(runPlan), concurrency);
		}
		res.writeXml("OK");
	}

	/**
	 * Deletes a document for the given Id.
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 */
	private void deleteDocument(Request req, Response res, AccountInfo acc) throws SystemFault, ApplicationFault{
		String id = req.getString("key", true, true, false);
		boolean concurrency = true;
		IndexWriter.getInstance().delete(acc.name, id, concurrency);
		
		res.writeXml("OK");
	}
	
	/**
	 * Deletes a document for the given Id.
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 */
	private void deleteDocuments(Request req, Response res, AccountInfo acc) throws SystemFault, ApplicationFault{
		String ids = req.getString("keys", true, true, false);
		List<String> allIds = StringUtils.fastSplit(ids, ',');
		
		IndexWriter.getInstance().delete(acc.name, allIds);
		res.writeXml("OK");
	}	

	/**
	 * Searches for a query
	 * @param req
	 * @param res
	 * @throws ApplicationError
	 * @throws ApplicationFault
	 * @throws IOException
	 */
	private void searchDocument(Request req, Response res, AccountInfo acc) 
		throws ApplicationFault, SystemFault { 

		String query = req.getString("query", true, true, false);
		QueryContext ctx = new QueryContext(acc, query);

		Boolean matchTags = req.getBoolean("tags", false);
		if (  null != matchTags ) ctx.matchTags = true;
		

		if (! StringUtils.isEmpty(req.getString("user", false,true,true)) ) {
			Object userObj = req.getObject("user", false);
			if ( null != userObj) ctx.user = (WhoAmI) userObj;
		}
		
		if ( ! StringUtils.isEmpty(req.clientIp) ) ctx.ipAddress = req.clientIp;

		QueryResult results = null;
		results = IndexReader.getInstance().search(ctx);
		
		int size = ( null == results) ? 0 : 
			( null == results.teasers) ? 0 : results.teasers.length;
		if ( 0 == size) {
			res.writeXml("<list></list>");
			return;
		}
		
		results.toXml(res.getWriter());
	}
	
	private void lookupDictionary(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);

		word = word.toLowerCase();
		String stemWord = Stemmer.getInstance().stem(word);
		DictEntry entry = DictionaryManager.getInstance().get(acc.name, stemWord);
		
		if ( null == entry ) {
			entry = DictionaryManager.getInstance().get(acc.name, word);
			if ( null == entry ) {
				res.writeXml("<r>none</r>");
				return;
			}
		}
		
		try {
			entry.toXml(res.getWriter());
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}
	
	private void refreshDictionary(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		DictionaryManager.getInstance().refresh(acc.name);
		res.writeXml("OK");
	}	
	
	private void listDictionary(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		
		String indexLetter = req.getString("index.letter", false, false, true);
		if ( StringUtils.isEmpty(indexLetter)) {
			indexLetter = StringUtils.Empty;
		}
		
		Writer writer = res.getWriter();
		
		res.writeHeader();
		DictionaryManager.getInstance().getKeywords(acc.name,indexLetter, writer);
		res.writeFooter();
	}
	
	private void addDictionary(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		DictEntry entry = (DictEntry) req.getObject("entry", true);

		String separator = req.getString("separator", false,true,false);
		separator = parseSeparator(separator);
		
		entry.type = entry.type.replace(separator, DictEntry.TYPE_SEPARATOR); 
		
		DictionaryManager.getInstance().add(acc.name, entry);
		res.writeXml("OK");
	}
	
	@SuppressWarnings("unchecked")
	private void addBatchDictionary(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		
		String entriesXml = req.getString("entries", true, true, false);
		List<DictEntry> entries = (List<DictEntry>) 
			XmlUtils.xstream.fromXML(StringEscapeUtils.unescapeXml(entriesXml));
		
		Hashtable<String, DictEntry> hashEntries = new 
			Hashtable<String, DictEntry>(entries.size());
		
		for (DictEntry aEntry : entries) {
			hashEntries.put(aEntry.word, aEntry);
		}
		DictionaryManager.getInstance().add(acc.name, hashEntries);
		res.writeXml("OK");
	}

	private void spell(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);
		List<String> words = DictionaryManager.getInstance().getSpelled(acc.name, word);
		if ( null == words ) {
			res.writeXml("<r>none</r>");
			return;
		}
		
		res.writeXml(words);
	}
	
	private void regex(Request req, Response res, AccountInfo acc)
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);
		List<String> words = DictionaryManager.getInstance().getWildCard(acc.name, word);
		if ( null == words ) {
			res.writeXml("<r>none</r>");
			return;
		}
		
		res.writeXml(words);
	}

	private void delete(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		String words = req.getString("words", true, true, false);
		List<String> wordL = StringUtils.fastSplit(words, ',');
		DictionaryManager.getInstance().delete(acc.name, wordL);
		res.writeXml("OK");
	}

	private void deleteSpecial(Request req, Response res, AccountInfo acc) 
	throws ApplicationFault, SystemFault {
		String word = req.getString("word", true, true, false);
		List<String> words = DictionaryManager.getInstance().getWildCard(acc.name, word);
		DictionaryManager.getInstance().delete(acc.name, words);
		res.writeXml("OK");
	}
	
	private String parseSeparator(String separator) {
		if ( StringUtils.isEmpty(separator) ) return ", ";
		
		String separatorLower = separator.toLowerCase();
		if ( "tab".equals(separatorLower)) separator = "\t";
		else if ( "newline".equals(separatorLower)) separator = "\n";
		return separator;
	}	
}
