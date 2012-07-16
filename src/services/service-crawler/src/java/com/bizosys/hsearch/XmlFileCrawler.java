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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Field;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.XmlRecordExtracter;
import com.bizosys.hsearch.util.XmlRecordExtracterCallback;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

public class XmlFileCrawler implements XmlRecordExtracterCallback {
	
	private static Logger l = Logger.getLogger(XmlFileCrawler.class.getName());
	
	private int batchSize = 300;

	public Map<String, String> startTag = null;
	public Map<String,String> endTag = null;
	public String recordStartTag = null;
	public String recordEndTag = null;

	public int startIndex = 0;
	public int endIndex = -1;
	public boolean isEndIndex = false;

	public String docName = null;

	public File aFile = null;
	public boolean isTitle = false;
	public int titleIndexT = 0;
	public String idFldName = null;
	public String[] titleFldNames = null;
	public boolean isPreview = false;
	public int previewIndexT = 0;
	public String[] previewFldNames = null;
	public String dictKeepField = null;
	public String dictRecordType = null;
	public String urlPrefix = null;
	public String recordTag = null;

	private HDocument pristineDoc = null;
	private List<PipeIn> runPlan = null;
	
	private List<Map<String,String>> records = null;
	int readDocs = 0;
	boolean isMultiWriter = false;
	boolean firstTimeLoad = false;
	
	long bucket = -1L;
	
	String tenant = "anonymous";
	AccountInfo acc = null;
	
	/**
	 * 
	 * @param filePath
	 * @param idFieldName
	 * @param titleFieldNamed
	 * @param plan
	 * @throws ApplicationFault
	 */
	public XmlFileCrawler(
			String tenant, String filePath,
			HDocument doc, String idFldName, 
			String[] titleFldNames,
			String[] previewFields,  List<PipeIn> plan, 
			int startIndex, int endIndex, int batchSize, boolean firstTimeLoad, long startBucket) 
		throws ApplicationFault, SystemFault {
			if ( ! StringUtils.isEmpty(tenant)) this.tenant = tenant;
			
			this.acc = Account.getAccount(this.tenant);
			if ( null == acc) {
				acc = new AccountInfo(this.tenant);
				acc.name = this.tenant;
				acc.maxbuckets = 1;
				Account.storeAccount(acc);
			}
			
			this.pristineDoc = doc;
			this.recordTag = this.pristineDoc.docType; 
			this.aFile = FileReaderUtil.getFile(filePath);
			if ( !aFile.exists() ) {
				throw new ApplicationFault(
					"TabFileFetcher > File does not exist, " + this.aFile.getAbsolutePath());
			}
			if ( !aFile.canRead() ) {
				throw new ApplicationFault(
					"TabFileFetcher > Can't read the file" + this.aFile.getAbsolutePath());
			}
			
	    	String fileName = aFile.getName();
	    	int dotAt = fileName.lastIndexOf('.');
	    	String docName = ( dotAt == -1) ? fileName : fileName.substring(0, dotAt);
	    	
	    	this.recordStartTag = "<" + docName + ">"; 
	    	this.recordEndTag= "</" + docName + ">";
	    	
	    	this.idFldName = idFldName;
	    	this.titleFldNames = titleFldNames;
	    	this.previewFldNames = previewFields;
		
			if ( endIndex  != -1) {
				if ( startIndex >= endIndex ) throw new ApplicationFault(
					"Not allowed as reading ends at " + endIndex);
			}

			this.startIndex = startIndex;
			if ( endIndex != -1 && endIndex <= startIndex ) 
				throw new ApplicationFault("Not allowed as reading starts from " + startIndex);

			this.endIndex = endIndex;
			if ( this.endIndex > 0 ) isEndIndex = true;
		
			this.batchSize = batchSize;
			this.runPlan = ( null == plan) ? 
				IndexWriter.getInstance().getInsertPipes() : plan;
			this.firstTimeLoad = firstTimeLoad;
			this.bucket = startBucket;
	}


	public void fetchAndIndex() throws ApplicationFault, SystemFault {
		
		l.info("XmlFileFetcher > Loading " + this.aFile.getName());
		this.initialize();
		
		this.readDocs = -1;
		InputStream stream = null;
		try {
			
			this.records = new ArrayList<Map<String,String>>(400);
			XmlRecordExtracter xh = new XmlRecordExtracter(this.recordTag, this);
			SAXParser sp = SAXParserFactory.newInstance().newSAXParser();
			stream = new FileInputStream(this.aFile);
			sp.parse(stream, xh);
			this.processRecords(); //Handle the last remaining batch. All the other records wilb be processed through handleRecord method. 
		
		} catch (Exception ex) {
			l.fatal("XmlFileFetcher > Error in indexing file " + this.aFile.getName(), ex);
			throw new ApplicationFault(ex);
		
		} finally {
			try {
				if (null != stream) stream.close();
			}  catch (Exception ex) {
				l.warn("util.FileReaderUtil", ex);
			}
		}
		return;
	}

	/**
	 * This method is called from the record extractor XmlRecordExtracter. So if processing needs to be stopped it has to be informed of the same.
	 * 
	 */
	public boolean handleRecord(Map<String, String> fieldM) {
		this.records.add(fieldM);
		if (this.records.size() < this.batchSize) return true;
		boolean status = this.processRecords();
		for (Map<String, String> record : this.records) {
			ObjectFactory.getInstance().putStringMap(record);
		}
		this.records.clear();
		return status;
	}

	long start = 0;
	long end = 0;
	int total = 0;
	StringBuilder reportSb = new StringBuilder(100);
	
	private boolean processRecords() {
		try {
			start = System.currentTimeMillis();
			
			boolean status = this.insert();
	
			end = System.currentTimeMillis();
			reportSb.append("\nTotal records written " );
			reportSb.append(readDocs);
			reportSb.append(" , in " );
			int timeTaken = new Long(end - start).intValue();
			reportSb.append(timeTaken/this.batchSize);
			reportSb.append(" (ms)/rec and average, ");
			total = total + timeTaken ;
			reportSb.append(total/readDocs);
			reportSb.append(" (ms)/rec with status, ");
			reportSb.append(status);
			reportSb.delete(0, reportSb.capacity());
			return status;
		
		} catch (Exception e){
			e.printStackTrace();
			return false;
		}
	}

	private void initialize() throws ApplicationFault {
		
		if (StringUtils.isEmpty(this.idFldName))  
			throw new ApplicationFault("ID field is not known.");
		this.isTitle = (null != this.titleFldNames);
		if (isTitle) this.titleIndexT = this.titleFldNames.length;
		
		this.isPreview = (null != this.previewFldNames);
		if (isPreview) {
			this.startTag = new HashMap<String, String>();
			this.endTag = new HashMap<String, String>();;
			for (String previewField : this.previewFldNames) {
				this.startTag.put(previewField, ("<" + previewField + ">"));
				this.endTag.put(previewField, ("</" + previewField + ">"));
			}
			this.previewIndexT = this.previewFldNames.length;
		}
	}

	static Long id = 0L;
	static long batch = 0;
	
	private boolean insert() throws Exception {
		this.bucket++;
		boolean indexNext = true;
		StringBuilder text = new StringBuilder(1024);
		StringBuilder title = null;
		if ( isTitle ) title = new StringBuilder();
		StringBuilder xml = null;
		if ( isPreview ) xml = new StringBuilder(1024);
		
		List<HDocument> hdocs = new ArrayList<HDocument>(records.size());
		short docSerial = 1;
		for (Map<String,String> cols : this.records) {
			
			if (! cols.containsKey(this.idFldName)) {
				if ( l.isInfoEnabled())
					l.info("Missing ID field, skipping" + cols.keySet().toString());
				cols.put(this.idFldName, id.toString());
				id = id + 1;
				continue;
			}
			
			readDocs++;
			if ( readDocs < this.startIndex) continue;
			if ( isEndIndex ) {
				if ( readDocs > this.endIndex) {
					indexNext = false;
					break;
				}
			}

			/**
			 * Refresh and reuse the containers
			 */
			text.delete(0, text.capacity());
			if ( isTitle ) title.delete(0, title.capacity());
			if ( isPreview ) xml.delete(0, xml.capacity());
			
			HDocument aDoc = new HDocument();
			aDoc.docType = this.pristineDoc.docType;
			aDoc.url = this.pristineDoc.url;
			aDoc.eastering = this.pristineDoc.eastering;
			aDoc.team = this.pristineDoc.team;
			aDoc.editPermission = this.pristineDoc.editPermission;
			aDoc.ipAddress = this.pristineDoc.ipAddress;
			aDoc.locale = this.pristineDoc.locale;
			aDoc.northing = this.pristineDoc.northing;
			aDoc.securityHigh= this.pristineDoc.securityHigh;
			aDoc.sentimentPositive = this.pristineDoc.sentimentPositive;
			aDoc.viewPermission = this.pristineDoc.viewPermission;

			aDoc.key = cols.get(this.idFldName);
			
			if ( this.firstTimeLoad) {
				aDoc.bucketId = this.bucket;
				aDoc.docSerialId = docSerial++;
			}
			
			cols.remove(this.idFldName);
			if ( cols.size() > 0 ) aDoc.fields = new ArrayList<Field>(cols.size());
			for (String fldName : cols.keySet()) {
				String colVal = StringUtils.encodeXml(cols.get(fldName));
				if ( StringUtils.isEmpty(colVal) ) continue;
				aDoc.fields.add(new HField(fldName, colVal));
			}
			
			aDoc.cacheText = text.toString().trim();
			if (l.isDebugEnabled()) l.debug("Tab body text:" + aDoc.cacheText);
			if ( isTitle) aDoc.title = buildTitle(title, cols);
			if ( isPreview ) aDoc.preview = this.buildPreview(xml, cols);
			hdocs.add(aDoc);
		}
		int amount = this.records.size();
		long s = System.currentTimeMillis();
		System.out.println("Incrementing : " + batch + " .. " + amount);
		batch = batch + amount;
		IndexWriter.getInstance().insertBatch(hdocs, this.acc, runPlan,isMultiWriter);
		long e = System.currentTimeMillis();
		System.out.println("Incremented For :" + this.records.size() + " in ms " + (e -s));
		return indexNext;
	}

	/**
	 * Populate the title
	 * 
	 * @param title
	 * @param cols
	 * @param content
	 */
	private String buildTitle(StringBuilder title, Map<String,String> fieldM) {
		
		String colVal = null;
		if (1 == this.titleIndexT) {
			return (fieldM.get(this.titleFldNames[0]));
		
		}  else {
			for (int titleI = 0; titleI < this.titleIndexT; titleI++) {
				colVal = fieldM.get(this.titleFldNames[titleI]);
				if (StringUtils.isEmpty(colVal)) continue;
				title.append(colVal);
				title.append(' ');
			}
			return title.toString().trim();
		}
	}

	/**
	 * Populate the Preview
	 */
	private String buildPreview(StringBuilder xml, 
		Map<String,String> fieldM) {
		
		String colVal = null;
		xml.append(this.recordStartTag);

		for (int previewI = 0; previewI < previewIndexT; previewI++) {
			
			colVal = fieldM.get(this.previewFldNames[previewI]);
			if (StringUtils.isEmpty(colVal)) continue;
			xml.append(this.startTag.get(this.previewFldNames[previewI]));
			xml.append(colVal);
			xml.append(this.endTag.get(this.previewFldNames[previewI]));
			
		}

		xml.append(this.recordEndTag);
		return xml.toString();
	}

	public boolean isMultiWriter() {
		return isMultiWriter;
	}

	public void setMultiWriter(boolean isMultiWriter) {
		this.isMultiWriter = isMultiWriter;
	}
}
