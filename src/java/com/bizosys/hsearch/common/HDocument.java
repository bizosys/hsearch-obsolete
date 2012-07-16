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
package com.bizosys.hsearch.common;

import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.util.GeoId;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;


/**
 * This object carries all information necessary for indexing a document.
 * This object is also serializable and client can provide it as a 
 * XML document (REST API).
 * @see GeoId
 */
public class HDocument {

	public static Logger l = CommonLog.l;
	private static final boolean DEBUG_ENABLED = l.isDebugEnabled();
	
	/**
	 * Document Merged Storage(Bucket) Number  
	 */
	public Long bucketId = null;
	
	/**
	 * Document Serial number inside the merged storage (Bucket)
	 */
	public Short docSerialId = null;
	

	/**
	 * This is the original Id of the document.
	 * This id usually flows from the original document source
	 * e.g. Primary Key of a database table. The mapped bucket Id and
	 * document serial number inside bucket represents uniqueness inside
	 * the index. 
	 */
	public String key =  null;
	
	/**
	 * URL for accessing the document directly
	 */
	public String url =  null;

	/**
	 * Document title. This also shows in the search result record title
	 */
	public String title =  null; 
	
	/**
	 * The Preview text on the document. It can be URL to an image or inline
	 * XML information.
	 */
	public String preview =  null;
	
	/**
	 * The matching section of the search word occurance is picked from
	 * the cached text sections
	 */
	public String cacheText =  null;

	/**
	 * Document content Fields
	 */ 
	public List<Field> fields = null;
	
	/**
	 * Manually supplied list of citation mentioned in the document
	 */
	public List<String> citationTo =  null;

	/**
	 * Manually supplied list of citations from other documents
	 */
	public List<String> citationFrom =  null;
	
	/**
	 * Who has view access to this document
	 */
	public AccessDefn viewPermission = null;
	
	/**
	 * Who has edit access of this document
	 */
	public AccessDefn editPermission = null;

	/**
	 * The state of the docucment (Applied, Processed, Active, Inactive)
	 */
	public String state = null;
	
	/**
	 * The tenant
	 */
	public String tenant = null;	
	
	/**
	 * Just the Organization Unit (HR, PRODUCTION, SI)
	 * If there are multi level separate it with \ or .
	 */
	public String team = null;

	/**
	 * Easting refers to the eastward-measured distance (or the x-coordinate)
	 * Use <code>GeoId.convertLatLng</code> method for getting nothing eastering
	 * from a given latitude and longitude.
	 */
	public Float eastering = 0.0f;

	/**
	 * northing refers to the northward-measured distance (or the y-coordinate). 
	 * Use <code>GeoId.convertLatLng</code> method for getting nothing eastering
	 * from a given latitude and longitude.
	 */
	public Float northing = 0.0f;

	/**
	 * This Default weight of the document. Few examples for computing the weight are
	 * <lu>
	 * 	<li>Editor assigned</li>
	 * 	<li>Default weight assigned to the document source e.g. pages from wikipedia.org</li>
	 * 	<li>Default weight assigned to the document editor e.g. blogs from CEO</li>
	 * </lu> 
	 * 
	 */
	public int weight = 0;

	/**
	 * Document Type. It's the record type.
	 * Use  <code>DocumentType</code> class to define default document types.
	 */
	public String docType = null;

	/**
	 * These are author keywords or meta section of the page
	 */
	public List<String> tags = null;

	/**
	 * These are user keywords formed from the search terms
	 */
	public List<String> socialText = null;

	/**
	 * Document creation date 
	 */
	public Date createdOn = null;

	/**
	 * Document updation date 
	 */
	public Date modifiedOn = null;
	
	/**
	 * When the document is scheduled to die or died
	 */
	public Date validTill = null;
	
	/**
	 * From which IP address is this document created. 
	 * This is specially for machine proximity ranking. 
	 */
	public String ipAddress = null;

	/**
	 * High Security setting. During high security, 
	 * the information kept encrypted. 
	 */
	public boolean securityHigh = false;

	
	/**
	 * By default the sentiment is positive. 
	 */
	public boolean sentimentPositive = true;
	
	/**
	 * Document Language. Default is English 
	 */
	public Locale locale = Locale.ENGLISH;
	
	private String hsearchKey = null;
	public String getTenantDocumentKey() throws ApplicationFault {
		if ( null != hsearchKey) return hsearchKey;
		hsearchKey = getTenantDocumentKey(this.tenant, key);
		return hsearchKey;
	}

	public static String getTenantDocumentKey(String tenant, String docId) throws ApplicationFault {
		if ( StringUtils.isEmpty(tenant) ) throw new ApplicationFault("Unknown tenant.");
		if ( StringUtils.isEmpty(docId) ) throw new ApplicationFault("Document Id not available.");
		return tenant + "/" + docId; 
	}

	public HDocument() {
		
	}
	
	public HDocument(String tenantName) {
		this.tenant = tenantName;
	}	
	
	/**
	 * Initialize with a key
	 * @param key	The Original Document Key
	 * @param tenantName	The Unique account  Name
	 */
	public HDocument(String key, String tenantName) {
		this.key = key;
		this.tenant = tenantName;
	}
	
	public void loadBucketAndSerials(AccountInfo acc) 
	throws ApplicationFault, SystemFault, BucketIsFullException {
		
		//Check for bucket id
		if ( null == this.bucketId) {
			if ( null != this.docSerialId) throw new ApplicationFault(
				"Bucker is absent while document position is present. Data corrupted.");
			this.bucketId = Account.getCurrentBucket(acc);
		} else {
			byte[] givenbucketB = Storable.putLong(this.bucketId);
			boolean illegalBucket = true; 
			for (byte[] allowedBucketB : acc.buckets) {
				if ( Storable.compareBytes(givenbucketB, allowedBucketB)) {
					illegalBucket = false;
					break;
				}
			}
			if ( illegalBucket ) {
				String msg = "User is not authorized to operate on " + 
					this.bucketId + " bucket.";
				l.warn(msg);
				throw new ApplicationFault( msg ); 
			}
				
		}
			
		if ( null == this.docSerialId) {
			try {
				this.docSerialId = Account.generateADocumentSerialId(this.bucketId);
			} catch (BucketIsFullException ex) {
				this.bucketId = Account.getNextBucket(acc);
				this.docSerialId = Account.generateADocumentSerialId(this.bucketId);
			}
		}
		if (DEBUG_ENABLED) l.debug("Bucket/DocPos : " + bucketId + "/" + docSerialId);
		
		this.tenant = acc.name;
	}
	
	public boolean validate() throws ApplicationFault {
		return !( null == key || null == tenant || null == bucketId || null == docSerialId);
	}
	
	public void recycle() {
		if ( null != citationTo) {
			citationTo.clear();
			citationTo =  null;
		}
		
		if ( null != citationFrom) {
			citationFrom.clear();
			citationFrom =  null;
		}
		
		if ( null != viewPermission ) {
			if ( null != viewPermission.getAccess()) viewPermission.getAccess().clear();
			viewPermission  =  null;
		}

		if ( null != editPermission ) {
			if ( null != editPermission .getAccess()) editPermission .getAccess().clear();
			editPermission   =  null;
		}

		if ( null != tags ) {
			tags.clear();
			tags  =  null;
		}

		if ( null != socialText ) {
			socialText.clear();
			socialText =  null;
		}
		
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("key=[").append(key).append("]\n");
		sb.append("tenant=[").append(tenant).append("]\n");
		sb.append("bucketId=[").append(bucketId).append("]\n");
		sb.append("docSerialId=[").append(docSerialId).append("]\n");
		sb.append("url=[").append(url).append("]\n");
		sb.append("title=[").append(title).append("]\n");
		return sb.toString();
		
	}
}
