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
import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;

/**
 * Stores the view and edit permission of a document.
 * An empty ACL only contains 4 bytes.
 * @author karan
 *
 */
public class DocAcl implements IDimension, IStorable {

	/**
	 * Who has edit access to the document
	 */
	public Access viewPermission = null;
	
	/**
	 * Who has view access to the document
	 */
	public Access editPermission = null;
	
	public DocAcl() {
	}
	
	public DocAcl(Access viewAcl, Access editAcl) {
		this.viewPermission = viewAcl;
		this.editPermission = editAcl;
	}
	
	public DocAcl(HDocument aDoc) {
		if ( null != aDoc.viewPermission)
			this.viewPermission = aDoc.viewPermission.getAccess();
		
		if ( null != aDoc.editPermission)
			this.editPermission = aDoc.editPermission.getAccess();
	}

	public DocAcl(byte[] bytes) {
		fromBytes(bytes, 0);
	}
	
	
	/**
	 * Read the ACL information from the byte array.
	 * Deserialize and initiate
	 * @param bytes : Serialized bytes
	 */
	public DocAcl(byte[] bytes, int pos) {
		fromBytes(bytes, pos);
	}

	public byte[] toBytes() {

		boolean isViewPerm = false; byte[] viewPermissionB = null;
		if ( null != this.viewPermission ) {
			isViewPerm = true;
			viewPermissionB = this.viewPermission.toStorable().toBytes();
		}
		boolean isEditPerm = false; byte[] editPermissionB = null;
		if ( null != this.editPermission ) {
			isEditPerm = true;
			editPermissionB = this.editPermission.toStorable().toBytes();
		}
		
		int totalBytes = 4; /** 2 + 2 the short lengths */
		if ( isViewPerm  ) totalBytes = totalBytes + viewPermissionB.length;
		if ( isEditPerm  ) totalBytes = totalBytes + editPermissionB.length;
		
		byte[] bytes = new byte[totalBytes];
		int pos = 0;
		
		short viewPermLen = ( isViewPerm) ? (short)viewPermissionB.length : (short) 0;
		System.arraycopy(Storable.putShort(viewPermLen), 0, bytes, pos, 2);
		pos = pos + 2;
		if ( isViewPerm) {
			System.arraycopy(viewPermissionB, 0, bytes, pos, viewPermLen);
			pos = pos+ viewPermLen;
		}
		
		short editPermLen = ( isEditPerm) ? (short)editPermissionB.length : (short) 0;
		System.arraycopy(Storable.putShort(editPermLen), 0, bytes, pos, 2);
		pos = pos + 2;
		if (isEditPerm) {
			System.arraycopy(editPermissionB, 0, bytes, pos, editPermLen);
			pos = pos+ editPermLen;
		}
		return bytes;
	}
	
	public int fromBytes(byte[] bytes, int pos) {
		short len = Storable.getShort(pos, bytes);
		pos = pos + 2;
		if ( 0 != len ) {
			byte[] viewAclB = new byte[len];
			System.arraycopy(bytes, pos, viewAclB, 0, len);
			this.viewPermission = new Access(viewAclB);
			pos = pos + viewAclB.length;
		}

		len = Storable.getShort(pos, bytes);
		pos = pos + 2;
		if ( 0 != len ) {
			byte[] editAclB = new byte[len];
			System.arraycopy(bytes, pos, editAclB, 0, len);
			this.editPermission = new Access(editAclB);
			pos = pos+ editAclB.length;
		}
		return pos;
	}	

	public void toNVs(List<NV> nvs) {
		if ( null == this.viewPermission && null == this.editPermission) return;
		nvs.add(new NV(IOConstants.SEARCH_BYTES,IOConstants.ACL_BYTES, this));
	}

	public void cleanup() {
		if ( null != this.viewPermission ) {
			this.viewPermission.clear();
			this.viewPermission = null;
		}
		if ( null != this.editPermission ) {
			this.editPermission.clear();
			this.editPermission = null;
		}
	}
	
	public void toXml(Writer writer) throws IOException {
		writer.append("<acl>");
		if ( null != this.viewPermission ) {
			writer.append("<view>");
			writer.append(this.viewPermission.toString());	
			writer.append("</view>");
		}
		
		if ( null != this.editPermission ) {
			writer.append("<edit>");
			writer.append(this.editPermission.toString());	
			writer.append("</edit>");
		}

		writer.append("</acl>");
	}
	
}
