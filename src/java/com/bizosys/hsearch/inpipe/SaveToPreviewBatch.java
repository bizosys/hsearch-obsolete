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

import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.index.PreviewAppendRecord;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.Record;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Persist the record information in preview table. In the batch mode, it expects
 * 1) Bunch comes under one bucket only  
 * 2) Each bucket will be overriden with no merging
 * 3) At one time one client only operates on a bucket. So thread safety is not provided. 
 * 
 * @author karan
 *
 */
public class SaveToPreviewBatch implements PipeIn {

	List<Doc> previews = null;
	boolean mergePreview = false; 
	public SaveToPreviewBatch() {
	}
	
	public SaveToPreviewBatch(boolean mergePreview) {
		this.mergePreview = mergePreview;
	}

	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;

		if ( null == doc.content) return;
		
		if ( null == previews) previews = ObjectFactory.getInstance().getDocumentList();
		this.previews.add(doc);
	}

	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( this.mergePreview) commitMerged(multiWriter);
		else commitSingular(multiWriter);
	}
	
	/**
	 * Creating the term bucket to save the changes.
	 */
	public void commitMerged(boolean multiWriter) throws ApplicationFault, SystemFault {

		if ( null == this.previews) return;
		
		/**
		 * Iterate through all content 
		 */
		Map<Integer, byte[]> metas = null;
		Map<Integer, byte[]> acls = null;
		Map<Integer, byte[]> teasers = null;

		Long bucketId = null;
		int docSerial = 0;
		List<NV> nvs = null;
		
		try {
			
			int maxDocSerialId = Short.MIN_VALUE;
			
			metas = ObjectFactory.getInstance().getByteBlockMap();
			acls = ObjectFactory.getInstance().getByteBlockMap();
			teasers = ObjectFactory.getInstance().getByteBlockMap();
			for (Doc doc : this.previews) {
				if ( null == bucketId ) bucketId = doc.bucketId;
				docSerial = doc.docSerialId;
				if ( maxDocSerialId < docSerial) maxDocSerialId = docSerial;
				if ( null == doc.meta ) continue;
				metas.put(docSerial, doc.meta.toBytes());
				acls.put(docSerial, doc.acl.toBytes());
				teasers.put(docSerial, doc.teaser.toBytes());
			}
			
			PreviewAppendRecord rec = new PreviewAppendRecord(
				new Storable(bucketId), metas,acls,teasers);
			nvs = ObjectFactory.getInstance().getNVList();
			Record record = new Record(rec.pk, rec.getNVs(nvs));
			HWriter.getInstance(multiWriter).insert( IOConstants.TABLE_PREVIEW, record);
			
		} catch (Exception ex) {
			InpipeLog.l.fatal("bucket Id : " + bucketId);
			throw new SystemFault("SaveToPreviewBatch : Failed.", ex);
		} finally {
			if (null != nvs) ObjectFactory.getInstance().putNVList(nvs);
			if ( null != previews) ObjectFactory.getInstance().putDocumentList(previews);
			if ( null != acls) ObjectFactory.getInstance().putByteBlockMap(acls);			
			if ( null != metas) ObjectFactory.getInstance().putByteBlockMap(metas);			
			if ( null != teasers) ObjectFactory.getInstance().putByteBlockMap(teasers);			
		}
	}
	
	/**
	 * Creating the term bucket to save the changes.
	 */
	@SuppressWarnings("unchecked")
	public void commitSingular(boolean multiWriter) throws ApplicationFault, SystemFault {

		if ( null == this.previews) return;
		
		/**
		 * Iterate through all content 
		 */
		List<Record> previewRecords = null;
		Object[] nvA = null;
		int nvI = 0;
		try {
			previewRecords = ObjectFactory.getInstance().getRecordList();
			nvA = new Object[this.previews.size()];
			for (Doc doc : this.previews) {
				if ( null == doc.content ) continue;
				String id = IdMapping.getBucket_DocPos(doc.bucketId, doc.docSerialId);
				List<NV> nvs =  ObjectFactory.getInstance().getNVList();
				nvA[nvI] = nvs;
				nvI++;
				
				doc.meta.toNVs(nvs);
				doc.acl.toNVs(nvs);
				doc.teaser.toNVs(nvs);
				previewRecords.add(new Record(new Storable(id),nvs));
				if ( InpipeLog.l.isDebugEnabled() ) 
					InpipeLog.l.debug("Adding to preview table :" + id);
			}
			HWriter.getInstance(multiWriter).insert(IOConstants.TABLE_PREVIEW, previewRecords);
		
		} catch (Exception ex) {
			throw new SystemFault("SaveToPreviewBatch : Failed.", ex);
		
		} finally {
			if ( null != nvA) {
				for (Object nvO : nvA) {
					if ( null == nvO) continue;
					List<NV> nvs = (List<NV>) nvO;
					ObjectFactory.getInstance().putNVList(nvs);
				}
			}
			
			if ( null != previews) 
				ObjectFactory.getInstance().putDocumentList(previews);
			ObjectFactory.getInstance().putRecordList(previewRecords);
		}
	}	

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.mergePreview = conf.getBoolean("preview.merge", false);
	}

	public PipeIn getInstance() {
		return new SaveToPreviewBatch(this.mergePreview);
	}

	public String getName() {
		return "SaveToPreviewBatch";
	}

}
