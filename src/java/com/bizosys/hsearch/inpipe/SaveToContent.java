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

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.Record;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Persists to detail table
 * @author karan
 *
 */
public class SaveToContent implements PipeIn {

	List<Doc> details = null;
	public SaveToContent() {
		
	}
	
	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		if ( null == docObj) return;
		Doc doc = (Doc) docObj;

		if ( null == doc.content) return;
		if ( null == details) details = ObjectFactory.getInstance().getDocumentList();
		this.details.add(doc);
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	@SuppressWarnings("unchecked")
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == this.details) return;
		
		/**
		 * Iterate through all content 
		 */
		List<Record> contentRecords = null;
		List[] docNvs = new List[this.details.size()];
		int counter = 0;
		ObjectFactory of = ObjectFactory.getInstance();

		try {
			contentRecords = of.getRecordList(); 
			for (Doc doc : this.details) {
				if ( null == doc.content ) continue;
				String id = IdMapping.getBucket_DocPos(doc.bucketId, doc.docSerialId);
				
				List<NV> nvs = of.getNVList();
				docNvs[counter] =  nvs;
				counter++;
				
				doc.content.toNVs(nvs);
				if ( nvs.size() > 0 ) contentRecords.add(new Record(new Storable(id),nvs));
			}
			if ( 0 == contentRecords.size()) return;
			HWriter.getInstance(multiWriter).insert(IOConstants.TABLE_CONTENT, contentRecords);
			return;
		} catch (Exception ex) {
			throw new SystemFault("SaveToContent : Failed", ex);
		} finally {
			for ( List nvList : docNvs ) {
				of.putNVList(nvList);
			}
			if ( null != contentRecords) of.putRecordList(contentRecords);
			if ( null != this.details) of.putDocumentList(this.details);
		}
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}

	public PipeIn getInstance() {
		return new SaveToContent();
	}

	public String getName() {
		return "SaveToContent";
	}

}
