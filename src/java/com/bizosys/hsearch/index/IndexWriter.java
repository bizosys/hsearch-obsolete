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
package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.PerformanceLogger;
import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.AutoIncrIdRange;
import com.bizosys.hsearch.common.BucketIsFullException;
import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.inpipe.ComputeIpHouse;
import com.bizosys.hsearch.inpipe.ComputeTokens;
import com.bizosys.hsearch.inpipe.ComputeUrlShortner;
import com.bizosys.hsearch.inpipe.DeleteFromContent;
import com.bizosys.hsearch.inpipe.DeleteFromDictionary;
import com.bizosys.hsearch.inpipe.DeleteFromIndex;
import com.bizosys.hsearch.inpipe.DeleteFromPreview;
import com.bizosys.hsearch.inpipe.FilterDuplicateId;
import com.bizosys.hsearch.inpipe.FilterLowercase;
import com.bizosys.hsearch.inpipe.FilterStem;
import com.bizosys.hsearch.inpipe.FilterStopwords;
import com.bizosys.hsearch.inpipe.FilterTermLength;
import com.bizosys.hsearch.inpipe.NormalizeAccents;
import com.bizosys.hsearch.inpipe.RemoveBlankSpace;
import com.bizosys.hsearch.inpipe.RemoveCachedText;
import com.bizosys.hsearch.inpipe.RemoveNonAscii;
import com.bizosys.hsearch.inpipe.SaveToBufferedDictionary;
import com.bizosys.hsearch.inpipe.SaveToContent;
import com.bizosys.hsearch.inpipe.SaveToDictionary;
import com.bizosys.hsearch.inpipe.SaveToIndex;
import com.bizosys.hsearch.inpipe.SaveToIndexBatch;
import com.bizosys.hsearch.inpipe.SaveToPreview;
import com.bizosys.hsearch.inpipe.SaveToPreviewBatch;
import com.bizosys.hsearch.inpipe.TokenizeNonEnglish;
import com.bizosys.hsearch.inpipe.TokenizeStandard;
import com.bizosys.hsearch.inpipe.TokenizeWhiteSpace;
import com.bizosys.hsearch.schema.ILanguageMap;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

/**
 * Performs write operation on HSearch index
 * @author karan
 *
 */
public class IndexWriter {

	private static final boolean INFO_ENABLED = IndexLog.l.isInfoEnabled();
	private static final boolean PERF_ENABLED = PerformanceLogger.l.isDebugEnabled();
	private static final boolean DEBUG_ENABLED = IndexLog.l.isDebugEnabled();
	private static IndexWriter singleton = null;
	public static IndexWriter getInstance() {
		if ( null != singleton) return singleton;
		synchronized (IndexWriter.class) {
			if ( null != singleton) return singleton;
			singleton = new IndexWriter();
		}
		return singleton;
	}
	
	private Map<String, PipeIn> writePipes = null; 

	/**
	 * Initializes the standard pipes
	 * Default private constructor
	 */
	private IndexWriter() {
	}
	
	public void init(Configuration conf) throws SystemFault, ApplicationFault{
		if ( null == writePipes) createPipes();
		for (PipeIn pipe: writePipes.values()) {
			pipe.init(conf);
		}
	}
	
	/**
	 * Creates standard sets of pipes
	 */
	private void createPipes() {
		if ( null != this.writePipes) return;
		
		this.writePipes = new HashMap<String, PipeIn>();
		
		FilterDuplicateId fdi = new FilterDuplicateId();
		this.writePipes.put(fdi.getName(), fdi);
		
		TokenizeStandard ts = new TokenizeStandard();
		this.writePipes.put(ts.getName(), ts);

		TokenizeNonEnglish tne = new TokenizeNonEnglish();
		this.writePipes.put(tne.getName(), tne);

		TokenizeWhiteSpace tws = new TokenizeWhiteSpace();
		this.writePipes.put(tws.getName(), tws);
		
		FilterStopwords fs = new FilterStopwords();
		this.writePipes.put(fs.getName(), fs);

		FilterTermLength ftl = new FilterTermLength();
		this.writePipes.put(ftl.getName(), ftl);

		FilterLowercase flc = new FilterLowercase();
		this.writePipes.put(flc.getName(), flc);

		FilterStem fstem = new FilterStem();
		this.writePipes.put(fstem.getName(), fstem);

		NormalizeAccents normAcc = new NormalizeAccents();
		this.writePipes.put(normAcc.getName(), normAcc);

		RemoveBlankSpace rbs = new RemoveBlankSpace();
		this.writePipes.put(rbs.getName(), rbs);

		RemoveNonAscii rna = new RemoveNonAscii();
		this.writePipes.put(rna.getName(), rna);

		ComputeTokens ct = new ComputeTokens();
		this.writePipes.put(ct.getName(), ct);

		ComputeIpHouse cih = new ComputeIpHouse();
		this.writePipes.put(cih.getName(), cih);

		ComputeUrlShortner cus = new ComputeUrlShortner();
		this.writePipes.put(cus.getName(), cus);

		SaveToIndex si = new SaveToIndex();
		this.writePipes.put(si.getName(), si);

		SaveToIndexBatch sib = new SaveToIndexBatch();
		this.writePipes.put(sib.getName(), sib);
		
		SaveToDictionary sd = new SaveToDictionary();
		this.writePipes.put(sd.getName(), sd);

		RemoveCachedText rct = new RemoveCachedText();
		this.writePipes.put(rct.getName(), rct);

		SaveToBufferedDictionary sdc = new SaveToBufferedDictionary();
		this.writePipes.put(sdc.getName(), sdc);

		SaveToPreview stp = new SaveToPreview();
		this.writePipes.put(stp.getName(), stp);

		SaveToPreviewBatch stpb = new SaveToPreviewBatch();
		this.writePipes.put(stpb.getName(), stpb);

		SaveToContent std = new SaveToContent();
		this.writePipes.put(std.getName(), std);

		DeleteFromIndex dfi = new DeleteFromIndex();
		this.writePipes.put(dfi.getName(), dfi);

		DeleteFromPreview dfpd = new DeleteFromPreview();
		this.writePipes.put(dfpd.getName(), dfpd);

		DeleteFromDictionary dfd = new DeleteFromDictionary();
		this.writePipes.put(dfd.getName(), dfd);
		
		DeleteFromContent dfc = new DeleteFromContent();
		this.writePipes.put(dfc.getName(), dfc);

	}
	
	public List<PipeIn> getPipes(String stepNames) throws ApplicationFault {
		
		if ( DEBUG_ENABLED)
			IndexLog.l.debug("IndexWriter: getPipes  = " + stepNames);
		
		if ( null == this.writePipes) createPipes();
		if ( null == stepNames) return getInsertPipes();
		
		String[] steps = StringUtils.getStrings(stepNames, ",");
		List<PipeIn> anvils = new ArrayList<PipeIn>(steps.length);
		for (String step : steps) {
			PipeIn basePipe = writePipes.get(step);
			if ( null == basePipe) {
				IndexLog.l.error("IndexWriter: getPipes Pipe not found =  " + step);
				throw new ApplicationFault("Pipe Not Found: " + step);
			}
			PipeIn aPipe = basePipe.getInstance();
			anvils.add(aPipe);
		}
		return anvils;
	}
	
	/**
	 * Following pipes are included in the standard write channel
	 * 
	 * FilterDuplicateId,TokenizeStandard,FilterStopwords,
	 * FilterTermLength,FilterLowercase,FilterStem,ComputeTokens,
	 * SaveToIndex,SaveToDictionary,SaveToPreview,SaveToContent
	 * @return	List of pipes
	 */
	public List<PipeIn> getInsertPipes() throws ApplicationFault {
		if ( null == this.writePipes) createPipes();
		return getPipes(
			"FilterDuplicateId,TokenizeStandard,FilterStopwords,"+
			"FilterTermLength,FilterLowercase,FilterStem,ComputeTokens," +
			"SaveToDictionary,SaveToIndex,SaveToPreview,SaveToContent");
	}
	
	public List<PipeIn> getDeletePipes() throws ApplicationFault {
		if ( null == this.writePipes) createPipes();
		return getPipes(
			"TokenizeStandard,FilterStopwords,FilterTermLength," +
			"FilterLowercase,FilterStem,ComputeTokens," +
			"DeleteFromIndex,DeleteFromPreview,DeleteFromDictionary,DeleteFromContent");
	}
	
	/**
	 * Insert one document applying the standard pipes 
	 * @param hdoc
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insert(HDocument hdoc, AccountInfo acc, boolean multiWriter) 
	throws ApplicationFault, SystemFault, BucketIsFullException {
		
		if ( null == acc) throw new ApplicationFault("Account is missing");
		if ( null == hdoc) throw new ApplicationFault("Document is missing");
		
		hdoc.tenant = acc.name;
		List<PipeIn> localPipes = getInsertPipes();
		insert(hdoc, acc, localPipes,multiWriter);
	}
	
	/**
	 * Insert a document with custom pipeline
	 * @param hdoc
	 * @param localPipes
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insert(HDocument hdoc, AccountInfo acc, List<PipeIn> localPipes, boolean multiWriter) 
	throws ApplicationFault, SystemFault, BucketIsFullException {

		if ( null == acc) throw new ApplicationFault("Account is missing");
		if ( null == hdoc) throw new ApplicationFault("Document is missing");
		
		hdoc.tenant = acc.name;
		hdoc.loadBucketAndSerials(acc);
		
		Doc doc = new Doc(hdoc);
		if ( DEBUG_ENABLED ) IndexLog.l.debug("Insert Step 1 > Value parsing is over.");
		
		for (PipeIn in : localPipes) {
			if ( DEBUG_ENABLED)
				IndexLog.l.debug("IndexWriter.insert.visitting : " + in.getName());
			in.visit(doc,multiWriter);
		}
		if ( DEBUG_ENABLED )IndexLog.l.debug("Insert Step 2 >  Pipe processing is over.");
		
		for (PipeIn in : localPipes) {
			if ( DEBUG_ENABLED)			
				IndexLog.l.debug("IndexWriter.insert.comitting :" + in.getName());
			in.commit(multiWriter);
		}
		if ( DEBUG_ENABLED ) IndexLog.l.debug("Insert Step 3 >  Commit is over.");
		doc.recycle();
	}

	/**
	 * Insert bunch of documents with standard pipelines
	 * @param hdocs
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insertBatch(List<HDocument> hdocs, AccountInfo acc, boolean multiWriter) 
	throws ApplicationFault, SystemFault, BucketIsFullException {

		List<PipeIn> localPipes = getInsertPipes();
		insertBatch(hdocs,acc,localPipes,multiWriter);
	}
	
	/**
	 * Insert bunch of documents with custom pipeline
	 * @param hdocs
	 * @param pipes
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public void insertBatch(List<HDocument> hdocs, AccountInfo acc, List<PipeIn> pipes, boolean multiWriter) 
	throws ApplicationFault, SystemFault, BucketIsFullException {

		if ( null == hdocs) return;
		if ( null == acc) throw new ApplicationFault("Account is missing");
		
		validateUniqueKeys(hdocs);
		int hDocsT = hdocs.size();
		List<Doc> docs = null;
		List<String> deleteDocIds = null;
		List<Integer> newDocPositions = null;
		
		try {
			docs = ObjectFactory.getInstance().getDocumentList();
			deleteDocIds = ObjectFactory.getInstance().getStringList();
			newDocPositions = ObjectFactory.getInstance().getIntegerList();
			
			/**
			 * Findout existing, replacable and new documents
			 */
			for (int i=0; i< hDocsT; i++) {
				HDocument hdoc = hdocs.get(i);
				hdoc.tenant = acc.name;

				IdMapping mapping = IdMapping.load(hdoc.tenant, hdoc.key);
				if ( null == mapping) {
					if ( null == hdoc.bucketId) newDocPositions.add(i);
				} else {
					/**
					 * We don't have a way to rewrite things. Pass to compaction.
					 * hdoc.bucketId = mapping.bucketId;
					 * hdoc.docSerialId =  mapping.docSerialId;
					 */
					deleteDocIds.add(hdoc.key);
				}
			}
			
			/**
			 * Delete duplicate documents in batch
			 */
			if ( deleteDocIds.size() > 0 ) delete(acc.name, deleteDocIds);
			
			
			int allocatedHDocs = 0;
			HDocument aHdoc = null;
			long curBucket = Account.getCurrentBucket(acc);

			int totalNewHDocs = newDocPositions.size();
			while ( allocatedHDocs < totalNewHDocs) {
				int remainingDocs = totalNewHDocs - allocatedHDocs;
				AutoIncrIdRange keyRanges = Account.
					generateAvailableDocumentSerialIds(curBucket, remainingDocs);
				
				short startPos = keyRanges.startPosition;
				for ( short i=0; i<keyRanges.totalAmount; i++) {
					int hDocPosition = newDocPositions.get(allocatedHDocs);
					aHdoc = hdocs.get(hDocPosition); 
					aHdoc.bucketId = curBucket;
					aHdoc.docSerialId =  new Integer(startPos + i).shortValue();
					allocatedHDocs++;
				}
				
				if ( allocatedHDocs == totalNewHDocs) break;
				curBucket = Account.getNextBucket(acc);
				if ( INFO_ENABLED) IndexLog.l.info(
					"Bucket is full.. Getting next Bucket , Remaining " + (totalNewHDocs - allocatedHDocs));
			}
			
			acc.refresh();
			/**
			 * Check and allocate loose end serial Ids
			 */
			for (HDocument hdoc : hdocs) {
				hdoc.loadBucketAndSerials(acc); 
				Doc doc = new Doc(hdoc);
				docs.add(doc);
			}
			if ( DEBUG_ENABLED ) IndexLog.l.debug("Insert Step 1 > Value parsing is over.");
			
			Map<PipeIn, Long> counters = null;
			if ( PERF_ENABLED) counters = new HashMap<PipeIn, Long>();
			long startTime = 0L,endTime = 0L;
	
			for (Doc doc : docs) {
				for (PipeIn in : pipes) {
					if ( in.getName().equals("FilterDuplicateId")) continue;
					if ( DEBUG_ENABLED) IndexLog.l.debug("IndexWriter.insert.visitting : " + in.getName());
					if ( PERF_ENABLED) {
						if ( startTime == 0 ) startTime = System.currentTimeMillis();
					}
					
					in.visit(doc,multiWriter);
					
					if ( PERF_ENABLED) {
						endTime = System.currentTimeMillis();
						if ( counters.containsKey(in))
							counters.put( in, ( counters.get(in) + (endTime - startTime) ) );
						else counters.put( in, (endTime - startTime) );
						startTime = endTime; 
					}
				}
			}
	
			if ( PERF_ENABLED){
				startTime = System.currentTimeMillis();
				for (PipeIn pipe : counters.keySet()) {
					PerformanceLogger.l.debug(pipe.getName() + " Visitting in ms > " + counters.get(pipe));
				}
				counters.clear();
			}
			
			for (PipeIn in : pipes) {
				if ( DEBUG_ENABLED) IndexLog.l.debug("IndexWriter.insert.comitting :" + in.getName());
				in.commit(multiWriter);
	
				if ( PERF_ENABLED) {
					endTime = System.currentTimeMillis();
					PerformanceLogger.l.debug(in.getName() + " Pipe Committing Execution Time:" + ( endTime - startTime) );
					endTime = startTime;
				}
				
			}
			if ( DEBUG_ENABLED ) IndexLog.l.debug("Insert Step 3 >  Pipe Commit.");
			
			for (Doc doc : docs) {
				doc.recycle();
			}
			
			
			for ( HDocument hdoc : hdocs) {
				hdoc.recycle();
			}
			
		} finally {
			if ( null != newDocPositions) ObjectFactory.getInstance().putIntegerList(newDocPositions);
			if ( null != docs) ObjectFactory.getInstance().putDocumentList(docs);
			if ( null != deleteDocIds) ObjectFactory.getInstance().putStringList(deleteDocIds);
		}
	}

	private void validateUniqueKeys(List<HDocument> hdocs) throws ApplicationFault {
		/**
		 * Sanity check for the supplied list
		 */
		Set<String> uniqueKeys = ObjectFactory.getInstance().getStringSet();
		for (HDocument hdoc : hdocs) {
			if ( null == hdoc) continue;

			if ( StringUtils.isEmpty(hdoc.key)) {
				ObjectFactory.getInstance().putStringSet(uniqueKeys);
				throw new ApplicationFault("Document Key is not present > " + hdoc.toString());
			}
			
			if ( uniqueKeys.contains(hdoc.key)) {
				ObjectFactory.getInstance().putStringSet(uniqueKeys);
				throw new ApplicationFault("Duplicate Document Key > " + hdoc.key);
			} else {
				uniqueKeys.add(hdoc.key);
			}
		}
		if ( null != uniqueKeys) ObjectFactory.getInstance().putStringSet(uniqueKeys);
	}
	
	/**
	 * 1 : Load the original document
	 * 2 : Parse the document 
	 * 2 : Remove From Dictionry, Index, Preview and Detail  
	 */
	public boolean delete(String tenant, String docId, boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( DEBUG_ENABLED) IndexLog.l.debug(
			"IndexWriter.delete : " + tenant + "/" + docId );

		Doc origDoc = IndexReader.getInstance().get(tenant, docId);
		return delete(tenant, multiWriter, origDoc);
	}

	public boolean delete(String tenant, boolean multiWriter, Doc origDoc) throws ApplicationFault, SystemFault {
		origDoc.tenant = tenant;
		if ( null != origDoc.content) {
			if ( null != origDoc.content.stored ) {
				origDoc.content.analyzedIndexed = origDoc.content.stored;
			}
		}

		List<PipeIn> deletePipe = getDeletePipes();
		
		if ( DEBUG_ENABLED) IndexLog.l.debug("Delete Step 1 > Value parsing is over.");
		
		for (PipeIn in : deletePipe) {
			if ( DEBUG_ENABLED)
				IndexLog.l.debug("IndexWriter.delete.visit : " + in.getName());
			in.visit(origDoc,multiWriter);
		}
		
		if ( DEBUG_ENABLED) IndexLog.l.debug("Delete Step 2 >  Pipe processing is over.");
		for (PipeIn in : deletePipe) {
			if ( DEBUG_ENABLED)
				IndexLog.l.debug("IndexWriter.delete.commit : " + in.getName());
			in.commit(multiWriter);
		}
		if ( DEBUG_ENABLED)
			IndexLog.l.debug(origDoc.bucketId + "/" + origDoc.docSerialId + " deleted.");
		
		return true;
	}
	
	public boolean delete(String tenant, List<String> docIds) throws ApplicationFault, SystemFault {
		
		int cache = 0;
		List<PipeIn> deletePipe = getDeletePipes();
		if ( DEBUG_ENABLED) IndexLog.l.debug("Delete Step 1 > Value parsing is over.");

		//final List<Doc> docs = new ArrayList<Doc>(docIds.size());
		final List<Doc> docs = ObjectFactory.getInstance().getDocumentList();
		
		try {
			for (String docId: docIds) {
				cache++;
				Doc origDoc = IndexReader.getInstance().get(tenant, docId);
				if ( null == origDoc.teaser) IndexLog.l.warn(docId + " has no teaser during delete loading.");
				origDoc.tenant = tenant;
				docs.add(origDoc);
				
				if ( cache >= 256) {
					deleteBatch(docs, deletePipe);
					cache = 0;
					docs.clear();
				}
			}
			if ( docs.size() > 0 ) deleteBatch(docs, deletePipe);
			return true;
		} finally {
			if ( null != docs ) ObjectFactory.getInstance().putDocumentList(docs);
		}
		
	}

	private void deleteBatch(final List<Doc> docs, List<PipeIn> deletePipe) throws ApplicationFault, SystemFault {
		if ( null == docs) return;
		if ( INFO_ENABLED ) IndexLog.l.info("Deleting " + docs.size() + " number of documents.");

		boolean skip = false;
		for (Doc aDoc : docs) {
			skip = false;
			if ( null == aDoc) continue;
			if ( null == aDoc.teaser) skip = true; 
			else if ( null == aDoc.teaser.id) skip = true;
			if (skip) {
				IndexLog.l.warn("IndexWriter:deleteBatch Skipping " + aDoc.toString());
				continue;
			}
			
			for (PipeIn in : deletePipe) {
				if ( DEBUG_ENABLED) IndexLog.l.debug("IndexWriter.delete.visit : " + in.getName());
				in.visit(aDoc,true);
			}
		}
		if ( DEBUG_ENABLED) IndexLog.l.debug("Delete Step 2 >  Pipe processing is over.");
		for (PipeIn in : deletePipe) {
			if ( DEBUG_ENABLED) IndexLog.l.debug("IndexWriter.delete.commit : " + in.getName());
			in.commit(true);
		}
	}	
	
	public void truncate(String apiKey) throws ApplicationFault, SystemFault {
		/**
		 * Now reset the buckets and it's counter
		 */
		AccountInfo accInfo = Account.getAccount(apiKey);
		if ( StringUtils.isEmpty(accInfo.name)) return;
		
		if ( null == accInfo.buckets) {
			if ( INFO_ENABLED) IndexLog.l.info(" In buckets information found > " + accInfo.toXml());
			return;
		}
		if ( INFO_ENABLED) IndexLog.l.info(accInfo.name + " total buckets found > " + accInfo.buckets.size());
		
		try {
			HDML.truncateBatch(IOConstants.TABLE_DICTIONARY, accInfo.name);
			
			for (char table : ILanguageMap.ALL_TABLES) {
				HDML.truncateBatch(new String(new char[]{table}), accInfo.buckets);
			}
			HDML.truncateBatch(IOConstants.TABLE_PREVIEW, accInfo.buckets);
			HDML.truncateBatch(IOConstants.TABLE_IDMAP, accInfo.name);
			
			for (byte[] bucket : accInfo.buckets) {
				Long bucketId = Storable.getLong(0, bucket);
				HDML.truncateBatch(IOConstants.TABLE_CONTENT, bucketId.toString());
				Account.resetDocumentCounter(Storable.getLong(0, bucket) );
			}
		} catch (Exception ex) {
			throw new ApplicationFault(ex);
		}
		
		/**
		 * Now reset the current bucket and allocated bucket just to first one
		 * TODO:// We are wasting already allocated bucket id. This needs to be reused.
		 */
		int bucketsT = accInfo.buckets.size();

		//Just leave only First bucket
		for ( int i=1; i< bucketsT; i++ ) {
			accInfo.buckets.remove(1);
		}

		if ( accInfo.buckets.size() == 1) {
			accInfo.curBucket = ByteField.getLong(0, accInfo.buckets.get(0));
			if ( INFO_ENABLED) IndexLog.l.info("Current bucket is set to : " + accInfo.curBucket);
		}
		Account.storeAccount(accInfo);
		
		//Cleanup type codes
		TermType.getInstance(true).truncate(accInfo.name);
		DocumentType.getInstance().truncate(accInfo.name);
		WeightType.getInstance(true).truncate(accInfo.name);

		if ( INFO_ENABLED) IndexLog.l.info(accInfo.name + " is truncated.");
	}


	public void truncate(String tenant, long bucket) throws ApplicationFault, SystemFault {
		
		if ( INFO_ENABLED) IndexLog.l.info("Truncating Tenant:" + tenant + "/" + bucket); 

		//Keep all unique document positions
		Set<Short> docPositions = new HashSet<Short>();
		List<InvertedIndex> ii = IndexReader.getInvertedIndex(bucket);
		if ( null != ii) {
			for (InvertedIndex index : ii) {
				if ( null == index.docPos) continue;
				for (short pos : index.docPos) {
					docPositions.add(pos);
				}
			}
		}
		
		if ( INFO_ENABLED ) IndexLog.l.info("Deleting " + 
			docPositions.size() + " documents from bucket : " + bucket);
		
		int cache = 0;
		List<PipeIn> deletePipe = getDeletePipes();
		List<Doc> docs = null;
		try {
			docs = ObjectFactory.getInstance().getDocumentList();
			for (Short docPos: docPositions) {
				cache++;
				Doc origDoc = new Doc(bucket, docPos,true);
				origDoc.tenant = tenant;
				docs.add(origDoc);
				
				if ( cache >= 256) {
					deleteBatch(docs, deletePipe);
					cache = 0;
					docs.clear();
				}
			}
			if ( docs.size() > 0 ) deleteBatch(docs, deletePipe);
		} finally {
			if ( null != docs ) ObjectFactory.getInstance().putDocumentList(docs);
		}		
		
		IStorable pk = new Storable(bucket);
		try {
			for (char table : ILanguageMap.ALL_TABLES) {
				if ( DEBUG_ENABLED ) IndexLog.l.debug("Deleting Bucket Row " + bucket + " from table " + table);
				HWriter.getInstance(false).delete(new String(new char[]{table}), pk);
			}
			HWriter.getInstance(false).delete(IOConstants.TABLE_PREVIEW, pk);
		} catch (IOException ex) {
			IndexLog.l.fatal("Error During deleting bucket " + 
				bucket + " for tenant " + tenant, ex);
			throw new SystemFault(ex);
		}
		
		Account.resetDocumentCounter(bucket);
	}
	
	/**
	private void truncateUsingIdMappingTable(String tenant) throws SystemFault, ApplicationFault {
		if ( INFO_ENABLED) IndexLog.l.info("Truncating Tenant:" + tenant); 
		List<String> tenantDocIds = 
			new TenantDocuments().getAllDocuments(tenant);

		if ( null == tenantDocIds) {
			if ( INFO_ENABLED) IndexLog.l.info("No documents found for Tenant:" + tenant);
		} else {
			IndexLog.l.info("Total Documents found: " + tenant + "=" + tenantDocIds.size());
			for (String aDocId: tenantDocIds) {
				if ( DEBUG_ENABLED ) IndexLog.l.debug("Deleting Document : " + aDocId);
				
				int index = aDocId.indexOf('_');
				if ( index < 1) {
					IndexLog.l.warn("truncate> Bad Id :" + aDocId + " , tenant " + tenant);
					continue;
				}
				Doc doc = new Doc(
					new Long(aDocId.substring(0, index)),
					new Short(aDocId.substring(index+1)),true);
				IndexWriter.getInstance().delete(tenant, true, doc);
			}
		}
	}
	*/
		
}
