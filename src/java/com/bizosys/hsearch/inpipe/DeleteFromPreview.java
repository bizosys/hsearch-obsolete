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
import java.util.Set;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.PreviewDeleteRecord;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Delete document from preview records
 * @author karan
 *
 */
public class DeleteFromPreview implements PipeIn {

	private static final boolean DEBUG_ENABLED = InpipeLog.l.isDebugEnabled();
	List<Doc> docs = null;
	
	public DeleteFromPreview() {
	}
	
	public void visit(Object objDoc, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) throw new ApplicationFault("No document");
		Doc doc = (Doc) objDoc;
		
		if ( null == docs ) docs = ObjectFactory.getInstance().getDocumentList();
		if ( null != doc.bucketId && null != doc.docSerialId ) {
			docs.add(doc);
			return;
		} else {
			InpipeLog.l.fatal("DeleteFromPreview: Document Original Id or Bucket Id with doc serial is absent.");
			throw new ApplicationFault ("DeleteFromPreview: No Ids provided for deletion.");
		}
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( DEBUG_ENABLED ) InpipeLog.l.debug("DeleteFromPreview: Commit Merged");

		if ( null == this.docs) return;
		
		Set<Long> uniqueBuckets = null;
		List<Short> serials = null;
		
		try {
			uniqueBuckets = ObjectFactory.getInstance().getLongSet();
			getUniqueBuckets(uniqueBuckets);
			serials = ObjectFactory.getInstance().getShortList();
			
			for (long bucket : uniqueBuckets) {
				serials.clear();
				for (Doc doc : docs) {
					if ( doc.bucketId != bucket ) continue;
					serials.add(doc.docSerialId);
				}
				PreviewDeleteRecord rec = new PreviewDeleteRecord(new Storable(bucket),serials);
				HWriter.getInstance(multiWriter).merge(IOConstants.TABLE_PREVIEW, rec);
			}
			
		} catch (Exception ex) {
			throw new SystemFault(ex);
		} finally {
			ObjectFactory of = ObjectFactory.getInstance();
			if ( null != uniqueBuckets ) of.putLongSet(uniqueBuckets);
			if ( null != serials ) of.putShortList(serials);
			if ( null != docs ) of.putDocumentList(docs);
		}
	}
	
	private  Set<Long> getUniqueBuckets(Set<Long> uniqueBuckets) throws ApplicationFault {
		if ( null == this.docs) return null;
		for (Doc doc : this.docs) {
			if ( null == doc) continue;
			uniqueBuckets.add(doc.bucketId);
		}
		return uniqueBuckets;
	}	
	
	public void init(Configuration conf) {
	}

	public PipeIn getInstance() {
		return new DeleteFromPreview();
	}

	public String getName() {
		return "DeleteFromPreview";
	}

}
