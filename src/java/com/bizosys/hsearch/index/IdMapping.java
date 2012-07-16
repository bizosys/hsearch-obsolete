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
import java.util.List;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.RecordScalar;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

/**
 * As multiple data format can be stored, an internal unique key is generated.
 * This unique key is based on the stored "bucket Id" and 
 * "Document Serial number" inside the bucket.  
 * 
 * This class defines the mapping relationship amoing the original key 
 * and the created bucket + serial key.
 * @author karan
 *
 */
public class IdMapping {
	
	public byte[] uniqueDocIdB = null;
	public Long bucketId;
	public Short docSerialId;
	
	public IdMapping(Long bucketId,Short docSerialId ) {
		this.bucketId = bucketId;
		this.docSerialId = docSerialId;
	}	
	
	public IdMapping(String tenant, String docId, 
	Long bucketId, Short docSerialId ) throws ApplicationFault {
		
		String uniqueDocId = HDocument.getTenantDocumentKey(tenant, docId);
		this.uniqueDocIdB = Storable.putString ( uniqueDocId );
		this.bucketId = bucketId;
		this.docSerialId = docSerialId;
	}
	
	public static IdMapping load(String tenant, String origDocId) 
	throws ApplicationFault, SystemFault{
		
		String uniqueDocId = HDocument.getTenantDocumentKey(tenant, origDocId);
		byte[] originalIdB = Storable.putString ( uniqueDocId );
		
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
				IOConstants.NAME_VALUE_BYTES );
		
		RecordScalar scalar = new RecordScalar(originalIdB,nv);
			
		HReader.getScalar(IOConstants.TABLE_IDMAP, scalar);
		if ( null == scalar.kv.data) return null;
		
		String key = new String(scalar.kv.data.toBytes());
		
		int foundAt = key.indexOf('_');
		if ( foundAt == -1) throw new ApplicationFault("IdMapping: Illegal Key:" + key);
		Long bucketId = new Long(key.substring(0,foundAt));
		Short docSerial = new Short(key.substring(foundAt+1));
		return new IdMapping(tenant, origDocId,bucketId,docSerial);
	}
	
	public void build(List<RecordScalar> records) {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
			IOConstants.NAME_VALUE_BYTES, 
			new Storable(getBucket_DocPos(this.bucketId, this.docSerialId) ) );
		
		RecordScalar record = new RecordScalar(this.uniqueDocIdB,nv);
		records.add(record);
	}
	
	/**
	 * Multiple writers at work.
	 * @param records
	 * @param concurrency
	 * @throws SystemFault
	 */
	public static final void persist(List<RecordScalar> records, boolean concurrency ) throws SystemFault{
		try {
			HWriter.getInstance(concurrency ).insertScalar(IOConstants.TABLE_IDMAP, records);
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}
	
	/**
	 * Delete a bucket ID from the mapping collection.
	 * @param bucketId	Bucket Id
	 * @param docSerialId	Document Serial Id
	 * @param concurrency	True if multiple writers at work
	 * @throws SystemFault	System Fault
	 */
	public static final void delete(String tenant, String docId, boolean concurrency) 
	throws SystemFault, ApplicationFault {
		try {
			String pk = HDocument.getTenantDocumentKey(tenant, docId);
			Storable pkB = new Storable(pk);
			HWriter.getInstance(concurrency).delete(
				IOConstants.TABLE_IDMAP, pkB);
		} catch (IOException ex) {
			throw new SystemFault(ex);
		}
	}
	
	public String getKey() {
		StringBuilder mappedKey = new StringBuilder(14);
		mappedKey.append(this.bucketId).append('_');
		mappedKey.append(this.docSerialId);
		return mappedKey.toString();
	}	
	
	
	public static final String getBucket_DocPos(
	String tenant, String docId) throws SystemFault, ApplicationFault{
		String mappedKey = HDocument.getTenantDocumentKey(tenant, docId);
		byte[] pk = ByteField.putString(mappedKey);
		List<NVBytes>  mappingB = HReader.getCompleteRow(IOConstants.TABLE_IDMAP, pk);
		if ( null == mappingB || 0 == mappingB.size() ) 
			throw new ApplicationFault("Id not found : " + tenant + "/" + docId);
		
		String bucketDocPos = new String(mappingB.get(0).data);
		return bucketDocPos;
		
	}
	
	public static final String getBucket_DocPos(long bucket, short docPos) {
		StringBuilder mappedKey = new StringBuilder(20);
		mappedKey.append(bucket).append('_');
		mappedKey.append(docPos);
		return mappedKey.toString();
	}
	
	public static final long getBucket(String key) throws ApplicationFault {
		int foundAt = key.indexOf('_');
		if ( foundAt == -1) throw new ApplicationFault("IdMapping: Illegal Key:" + key);
		return new Long(key.substring(0,foundAt));
	}
	
	public static final short getDocPos(String key) throws ApplicationFault {
		int foundAt = key.indexOf('_');
		if ( foundAt == -1) throw new ApplicationFault("IdMapping: Illegal Key:" + key);
		return new Short(key.substring(foundAt+1));
	}

}