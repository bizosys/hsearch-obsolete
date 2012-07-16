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

import java.io.IOException;
import java.util.List;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

/**
 * Delete document from preview records
 * @author karan
 *
 */
public class DeleteFromContent implements PipeIn {

	private List<String> contentTableIds = null;
	
	public DeleteFromContent() {
	}
	
	public void visit(Object objDoc, boolean multiWriter) throws ApplicationFault, SystemFault {
		
		if ( null == objDoc) throw new ApplicationFault("No document");
		Doc doc = (Doc) objDoc;
		
		if ( null == doc.teaser) throw new ApplicationFault("Document teaser section is not found");
		if ( StringUtils.isEmpty(doc.teaser.id)) 
			throw new ApplicationFault("Document Id is not found");
		
		if ( StringUtils.isEmpty(doc.tenant)) 
			throw new ApplicationFault("Document teant is not found");
		
		
		if ( null == contentTableIds ) contentTableIds = 
			ObjectFactory.getInstance().getStringList();
		contentTableIds.add(HDocument.getTenantDocumentKey(doc.tenant, doc.teaser.id));
		
	}

	/**
	 * Creating the term bucket to save the changes.
	 */
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		InpipeLog.l.debug("DeleteFromContent: Commit");
		int total = (null == this.contentTableIds ) ? 0 : this.contentTableIds.size();
		if ( 0 == total) return;
		
		String contentId = null;
		try {
			HWriter writer = HWriter.getInstance(multiWriter);
			for (int i=0; i< total; i++) {
				contentId = contentTableIds.get(i);
				writer.delete(IOConstants.TABLE_CONTENT, new Storable(contentId));
			}
		} catch (IOException ex) {
			StringBuilder sb = new StringBuilder(100);
			sb.append("DeleteFromContent: Failed deleting Id= " + contentId);
			throw new SystemFault(sb.toString(), ex);
		} finally {
			if ( null != contentTableIds )
				ObjectFactory.getInstance().putStringList(contentTableIds);
		}
	}
	
	
	public void init(Configuration conf) {
	}

	public PipeIn getInstance() {
		return new DeleteFromContent();
	}

	public String getName() {
		return "DeleteFromContent";
	}
	
}
