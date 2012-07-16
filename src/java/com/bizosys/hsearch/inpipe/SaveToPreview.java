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
import java.util.Set;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IndexLog;
import com.bizosys.hsearch.index.PreviewAppendRecord;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Persist to preview table
 * @author karan
 *
 */
public class SaveToPreview implements PipeIn {

	List<Doc> previews = null;
	boolean mergePreview = false; 
	public SaveToPreview() {
	}
	
	public SaveToPreview(boolean mergePreview) {
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

		if ( null == this.previews) return;
		
		/**
		 * Iterate through all content 
		 */
		Map<Integer, byte[]> metas = null;
		Map<Integer, byte[]> acls = null;
		Map<Integer, byte[]> teasers = null;

		int docSerial = 0;
		Set<Long> uniqueBuckets = ObjectFactory.getInstance().getLongSet();;
		
		try {
			metas = ObjectFactory.getInstance().getByteBlockMap();
			acls = ObjectFactory.getInstance().getByteBlockMap();
			teasers = ObjectFactory.getInstance().getByteBlockMap();
			
			/**
			 * Findout unique buckets
			 */
			for (Doc doc : this.previews) {
				uniqueBuckets.add(doc.bucketId);
			}
			
			/**
			 * Iterate each bucket and save information
			 */
			for (long bucketId : uniqueBuckets) {
				metas.clear();
				acls.clear();
				teasers.clear();

				for (Doc doc : this.previews) {
					if ( doc.bucketId != bucketId) continue; //Skip
					docSerial = doc.docSerialId;
					if ( null == doc.meta ) continue;
					metas.put(docSerial, doc.meta.toBytes());
					acls.put(docSerial, doc.acl.toBytes());
					teasers.put(docSerial, doc.teaser.toBytes());
				}
				
				PreviewAppendRecord rec = new PreviewAppendRecord(
					new Storable(bucketId), metas,acls,teasers); 
				
				HWriter.getInstance(multiWriter).merge(IOConstants.TABLE_PREVIEW, rec);
				
			}

		} catch (Exception ex) {
			if ( null != uniqueBuckets) {
				IndexLog.l.fatal("Failed while processing Buckets : " + uniqueBuckets.toString());
			}
			throw new SystemFault("SaveToPreview : Failed.", ex);
		} finally {
			if ( null != uniqueBuckets) ObjectFactory.getInstance().putLongSet(uniqueBuckets);
			if ( null != previews) ObjectFactory.getInstance().putDocumentList(previews);
			if ( null != acls) ObjectFactory.getInstance().putByteBlockMap(acls);			
			if ( null != metas) ObjectFactory.getInstance().putByteBlockMap(metas);			
			if ( null != teasers) ObjectFactory.getInstance().putByteBlockMap(teasers);			
		}
	}
	
	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.mergePreview = conf.getBoolean("preview.merge", false);
	}

	public PipeIn getInstance() {
		return new SaveToPreview(this.mergePreview);
	}

	public String getName() {
		return "SaveToPreview";
	}

}
