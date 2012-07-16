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
package com.bizosys.hsearch.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.Field;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

public class LuceneTabFileCrawler {
	private static Logger l = Logger.getLogger(LuceneTabFileCrawler.class.getName());

	private int betchSize = 300;
	
	private String[] fields = null;
	private String[] startTag = null;
	private String[] endTag = null;
	private String recordStartTag = null;
	private String recordEndTag = null;
	private int startIndex = 0;
	private int endIndex = -1;

	private int totalFields = 0;
	private int totalFieldsMinue1 = 0;
	private File aFile = null;

	private int idIndex = -1;
	private boolean isTitle = false;
	private int[] titleIndex = null;
	private int titleIndexT = 0;
	private String idFldName = null; 
	private String[] titleFldNames = null;	
	private boolean isPreview = false;
	private int[] previewIndex = null;
	private int previewIndexT = 0;
	private String[] previewFldNames = null;
	
	private HDocument pristineDoc = null;
	
	private boolean isEndIndex = false;
	int readDocs = 0;
	long bucket = 0;
	
	
	private LuceneTabFileCrawler() {
		
	}
	
	public LuceneTabFileCrawler(String filePath,
		HDocument doc, String idFldName, 
		String[] titleFldNames,
		String[] previewFields,  int startIndex, int endIndex, int batchSize) 
	throws ApplicationFault, SystemFault {

		this.pristineDoc = doc;
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
	
		this.betchSize = batchSize;
	}
	
	/**
	 * 
	 * @param runPlan
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void fetchAndIndex() throws ApplicationFault, SystemFault {
	
		l.debug("TabFileFetcher > Loading " + this.aFile.getName());
		readDocs = -1;
		
		BufferedReader reader = null;
		InputStream stream = null;
		String[] words = null;
		String line = null;
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			boolean isFirstLine = true;
			int counter = 0;
			List<String[]> records = new ArrayList<String[]>(50);
			while((line = reader.readLine()) !=null ) {
				if ( isFirstLine) {
					initialize(line);
					isFirstLine = false;
					continue;
				}
				if (line.length() == 0) continue;
				
				
				char first=line.charAt(0);
				switch (first) {
					case ' ' : case '\n' : case '#' :  // skip blank & comment lines
					continue;
				}
				
				counter = 0;
				int index1 = 0;
				int index2 = line.indexOf('\t');
				String token = null;
				words = new String[this.totalFields];
				while (index2 >= 0) {
					  token = line.substring(index1, index2);
					  if ( StringUtils.isEmpty(token)) words[counter] = StringUtils.Empty;
					  else words[counter] = token;
					  counter++;
					  index1 = index2 + 1;
					  if ( index1 > line.length() - 1) break;
					  index2 = line.indexOf('\t', index1);
				}
				if (index1 < line.length() - 1) words[counter] = line.substring(index1);
				if ( words[this.totalFieldsMinue1] == null ) words[this.totalFieldsMinue1] = StringUtils.Empty;
				records.add(words);

				int recordsT = records.size();
				if ( recordsT >= betchSize ) {
					l.debug("\n Total records written" + readDocs);
					if ( ! insert(records) ) return;
					records.clear();
				} 
	    	}
			
			this.insert(records);
			records.clear();
		} catch (Exception ex) {
			String msg = "";
			if ( null != line ) msg = line + "\n";
			if ( null != words) msg = msg + words.toString(); 
			
			l.fatal("TabFileFetcher > " + msg, ex);			
			throw new ApplicationFault(ex);
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {l.warn("TabFileFetcher", ex);}
			try {if ( null != stream ) stream.close();
			} catch (Exception ex) {l.warn("TabFileFetcher", ex);}
		}
		return;
	}
	
	private void initialize(String line) throws ApplicationFault {

		this.totalFieldsMinue1 = StringUtils.totalSighings(line, '\t');
		this.totalFields = this.totalFieldsMinue1 + 1;
		this.fields = StringUtils.getStrings(line, "\t");

		this.startTag = new String[this.totalFields];
		this.endTag = new String[this.totalFields];
		
		for ( int i=0; i< this.totalFieldsMinue1; i++ ) {
			this.startTag[i] = "<" + fields[i] + ">";
			this.endTag[i] = "</" + fields[i] + ">";
		}
		
		this.isTitle = null != this.titleFldNames;
		int titleCounter = 0;
		String commaSepTitles = null;
		if ( isTitle ) {
			this.titleIndex = new int[this.titleFldNames.length];
			commaSepTitles = StringUtils.arrayToString(this.titleFldNames);		
		}
		
		this.isPreview = null != this.previewFldNames;
		int previewCounter = 0;
		String commaSepPreviews = null;
		if ( isPreview ) {
			this.previewIndex = new int[this.previewFldNames.length];
			commaSepPreviews = StringUtils.arrayToString(this.previewFldNames);		
		}
		
		int counter = -1;
		
		
		for (String fld : this.fields) {
			counter++;

			//Id
			if ( this.idFldName.equals(fld) ) this.idIndex = counter;
			
			//Title
			if ( isTitle ) {
				if ( commaSepTitles.indexOf(fld) > -1 ) 
					this.titleIndex[titleCounter++] = counter;
			}

			//Preview
			if ( isPreview ) 
			{
				if (previewCounter < this.previewIndex.length && commaSepPreviews.indexOf(fld) > -1 ) 
					this.previewIndex[previewCounter++] = counter;
			}
		}
		if ( this.idIndex == -1) throw new ApplicationFault("ID field is not known.");

		if ( isTitle ) this.titleIndexT = this.titleIndex.length;
		if ( isPreview ) this.previewIndexT = this.previewIndex.length;

	}
		
	/**
	 * Insert
	 * @param records
	 * @param runPlan
	 * @return
	 * @throws Exception
	 */
	private boolean insert(List<String[]> records) throws Exception {
		
		bucket++;
		boolean indexNext = true;
		
		StringBuilder text = new StringBuilder(1024);
		
		StringBuilder title = null;
		if ( isTitle ) title = new StringBuilder();
		
		StringBuilder xml = null;
		if ( isPreview ) xml = new StringBuilder(1024);
		
		List<HDocument> hdocs = new ArrayList<HDocument>(records.size());
		for (String[] cols : records) {
			
			/**
			 * Boundary check.. Start and end points
			 */
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

			aDoc.key = cols[this.idIndex];
			aDoc.fields = new ArrayList<Field>(this.totalFieldsMinue1);
			for ( int i=0; i< this.totalFieldsMinue1; i++ ) {
				if ( this.idIndex == i) continue;
				if ( StringUtils.isEmpty(cols[i]) ) continue;
				String colVal = StringUtils.encodeXml(cols[i]).toLowerCase();
				if ( StringUtils.isEmpty(colVal) ) continue;
				aDoc.fields.add(new HField(this.fields[i], colVal));
				if (l.isDebugEnabled()) l.debug(
					"Field:" + this.fields[i] + "-----" + colVal);
				text.append(colVal).append(' ');
			}
			
			aDoc.cacheText = text.toString().trim();
			if ( isTitle) aDoc.title = buildTitle(title, cols);
			if ( isPreview ) aDoc.preview = this.buildPreview(xml, cols);
			hdocs.add(aDoc);
		}
		long s = System.currentTimeMillis();
		System.out.println("Inserting :" + records.size() + ", done:" + readDocs);
		LuceneIndexManager.getInstance().insert(hdocs);
		long e = System.currentTimeMillis();
		System.out.println("Total time taken :" + records.size() + " in ms " + (e -s));
		return indexNext;
	}

	/**
	 * Populate the title
	 * @param title
	 * @param cols
	 * @return
	 */
	private String buildTitle(StringBuilder title, String[] cols) {
		String colVal = null;
		if ( 1 == this.titleIndexT) {
			return cols[this.titleIndex[0]];
		} 
		
		for (int titleI=0; titleI<titleIndexT; titleI++ ) {
				colVal = cols[this.titleIndex[titleI]];
				if ( StringUtils.isEmpty(colVal) ) continue;
				title.append(colVal);
				title.append(' ');
		}
		return title.toString().trim();
	}

	/**
	 * Build the preview.
	 * @param xml
	 * @param cols
	 * @return
	 */
	private String buildPreview( StringBuilder xml, String[] cols) {
		
		String colVal = null;		
		xml.append(this.recordStartTag);
			
		int colIndex = 0;
		if ( 1 == this.previewIndexT) {
			colIndex = this.previewIndex[0];
			if ( StringUtils.isEmpty(cols[colIndex])) return null;
			colVal = StringUtils.encodeXml(cols[colIndex]);
			
			if ( ! StringUtils.isEmpty(colVal) ) {
				xml.append(this.startTag[colIndex]);
				xml.append(colVal);
				xml.append(this.endTag[colIndex]);
			}
			
		} else {
			for (int previewI=0; previewI<previewIndexT; previewI++ ) {
				colIndex = this.previewIndex[previewI];
				if ( StringUtils.isEmpty( cols[colIndex]) ) continue;
				colVal = StringUtils.encodeXml(cols[colIndex]);
				if ( StringUtils.isEmpty(colVal) ) continue;
				xml.append(this.startTag[colIndex]);
				xml.append(colVal);
				xml.append(this.endTag[colIndex]);
			}
		}
			
		xml.append(this.recordEndTag);
		return xml.toString();
	}
}
