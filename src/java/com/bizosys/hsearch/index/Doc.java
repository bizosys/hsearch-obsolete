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
import java.io.Writer;
import java.util.List;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.filter.PreviewFilterMerged;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.inpipe.util.ReaderType;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

/**
 * Documents are the unit of indexing and search. 
 * A Document consists of: 
 * <lu>
 * 	<li>Set of fields</li>
 * 	<li>Access information</li>
 * 	<li>Meta information</li>
 * 	<li>Result Display Section</li>
 * </lu>
 * <br/> * A document is uniquely identified by the doc merging Id (Bucket) 
 * and the document serial number inside the bucket.  
 * @author karan
 *
 */
public class Doc {
	
	/**
	 * Term vectors created after parsing the document
	 */
	public DocTerms terms = null;
	
	/**
	 * The document meta section
	 */
	public DocMeta meta = null;
	
	/**
	 * Document view and edit access control settings
	 */
	public DocAcl acl = null;
	
	/**
	 * The result display formats
	 */
	public DocTeaser teaser = null;
	
	/**
	 * The content section which consists of fields
	 */
	public DocContent content = null;
	
	/**
	 * From which machine the document is submitted
	 */
	public String ipAddress = null;
	
	/**
	 * The 
	 */
	public Long bucketId = null;
	public Short docSerialId = null;
	public String tenant = null;

	public Doc() {
	}
	
	public Doc(HDocument hDoc) throws SystemFault, ApplicationFault{
		if ( ! hDoc.validate()) { throw new ApplicationFault(
			"Invalid Document \n" + hDoc.toString());
		}
		this.tenant = hDoc.tenant; 
		this.bucketId = hDoc.bucketId;
		this.docSerialId = hDoc.docSerialId;
		this.ipAddress = hDoc.ipAddress;
		
		this.meta = new DocMeta(hDoc);
		this.teaser = new DocTeaser(hDoc);
		this.content = new DocContent(hDoc);
		this.acl = new DocAcl(hDoc);
		this.terms = new DocTerms();
	}
	
	public Doc(String tenant, String docId) throws SystemFault, ApplicationFault {
		
		/**
		 * Get the Bucket_Docpos
		 */
		String bucketDocPos = IdMapping.getBucket_DocPos(tenant, docId);
		this.bucketId = IdMapping.getBucket(bucketDocPos);
		this.docSerialId = IdMapping.getDocPos(bucketDocPos);
		
		/**
		 * Get the Content
		 */
		List<NVBytes> contentB = HReader.getCompleteRow(IOConstants.TABLE_CONTENT, bucketDocPos.getBytes());
		if ( null != contentB) {
			this.content = new DocContent(contentB );
			contentB.clear();
		}
		
		getPreviewMerged();
	}
	
	public Doc(long bucketId, short docSerialId, boolean mergePreview) throws SystemFault, ApplicationFault {
		if ( IndexLog.l.isDebugEnabled() ) {
			IndexLog.l.debug("Doc Initializing with : " + bucketId + "/" + docSerialId);
		}
		/**
		 * Get the Bucket_Docpos
		 */
		this.bucketId = bucketId;
		this.docSerialId = docSerialId;
		String bucketDocPos = IdMapping.getBucket_DocPos(this.bucketId, this.docSerialId);
		
		/**
		 * Get the Content
		 */
		List<NVBytes> contentB = HReader.getCompleteRow(IOConstants.TABLE_CONTENT, bucketDocPos.getBytes());
		if ( null != contentB) {
			this.content = new DocContent(contentB );
			contentB.clear();
		}
		
		getPreviewMerged();
	}
	
	
	private void getPreviewMerged() throws SystemFault, ApplicationFault {
		PreviewFilterMerged pfm = new PreviewFilterMerged(this.docSerialId);
		List<NVBytes> previewB = HReader.getCompleteRow(IOConstants.TABLE_PREVIEW, 
			Storable.putLong(this.bucketId), pfm);
		if ( null == previewB) return;
		
		for (NVBytes nv : previewB) {
			char name = new String(nv.name).charAt(0);
			if ( name == IOConstants.META_DETAIL_0 ) 
				this.meta = new DocMeta(nv.data);
			else if ( name == IOConstants.ACL_DETAIL_0 )
				this.acl = new DocAcl(nv.data);
			else if ( name == IOConstants.TEASER_DETAIL_0 )
				this.teaser = new DocTeaser(nv.data);
		}
		previewB.clear();
	}
	
	
	/**
	 * Recycles this document.
	 * Helps GC to garbase collect better.
	 *
	 */
	public void recycle() {
		this.terms.cleanup();
		this.meta.cleanup();
		this.acl.cleanup();
		this.teaser.cleanup();
		this.content.cleanup();
		bucketId = null;
		docSerialId = 0;
	}
	

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(">>>> Document Starts <<<<");
		if ( null != bucketId ) sb.append("\n Bucket :").append(bucketId.toString());
		if ( null != docSerialId ) sb.append("\n Doc Serial :").append(docSerialId);
		if ( null != tenant ) sb.append("\n Tenant :").append(tenant);
		if ( null != terms ) sb.append("\n Term :").append(terms.toString());
		if ( null != acl ) sb.append("\n Acl : ").append(acl.toString());
		if ( null != meta ) sb.append("\n Meta :").append(meta.toString());
		if ( null != teaser ) sb.append("\n Teaser:").append(teaser.toString());
		if ( null != content ) sb.append("\n Content").append(content.toString());
		sb.append("\n>>>> Document Ends <<<<\n");
		return sb.toString();
	}
	
	public void toXml(Writer writer) throws IOException {
		writer.append("<doc>");
		if ( null != tenant ) writer.append("<tenant>").append(tenant).append("</tenant>");
		if ( null != bucketId ) writer.append("<bucket>").append(bucketId.toString()).append("</bucket>");
		if ( null != docSerialId ) writer.append("<serial>").append(docSerialId.toString()).append("</serial>");
		if ( null != ipAddress ) writer.append("<ip>").append(ipAddress).append("</ip>");
		if ( null != acl ) acl.toXml(writer);
		if ( null != meta ) meta.toXml(writer);
		if ( null != teaser ) teaser.toXml(writer);
		if ( null != content ) content.toXml(writer);
		writer.append("</doc>");
	}
	
	public transient List<ReaderType> readers = null;
}
