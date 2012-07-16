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

import java.util.HashMap;
import java.util.Map;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

/**
 * Defines various document types.
 * To save indexing space, each document type is stored as a byte code.
 * So totally 256 diffierent document codes can be defined in the existing index.
 * However, multiplease documents codes may belong to a single type code.
 * In this case, more than 256 docs can be defined with multi docs falling under
 * same type codes.
 * @author karan
 *
 */
public class DocumentType extends TypeCode {
	
	public static String TYPE_KEY = "TYP";

	public static Byte NONE_TYPECODE = Byte.MIN_VALUE;
	
	public static DocumentType instance = null;
	public static DocumentType getInstance() throws SystemFault {
		if ( null != instance ) return instance;
		synchronized (DocumentType.class) {
			if ( null != instance ) return instance;
			instance = new DocumentType();
			return instance;
		}
	}
	
	private DocumentType() throws SystemFault {
	}
	

	@Override
	public void persist(String tenant, Map<String, Byte> types) throws SystemFault {
		byte[] key = (TYPE_KEY + tenant).getBytes();
		super.persist(tenant, types, key);
	}

	@Override 
	public Map<String, Byte> getDefaultCodes() throws SystemFault, ApplicationFault {
		Map<String, Byte> codes = new HashMap<String, Byte>(256);
		return codes;
	}
	
	public void append(String tenant, Map<String, Byte> types) throws SystemFault, ApplicationFault {
		Map<String, Byte> codes = load(tenant);
		if ( null == codes) codes = types;
		else codes.putAll(types);
		persist(tenant, codes);
	}
	
	public Map<String, Byte> load(String tenant) throws SystemFault, ApplicationFault {
		byte[] key = (TYPE_KEY + tenant).getBytes();
		return super.load(tenant, key);
	}
	
	@Override
	public void truncate(String tenant) throws SystemFault, ApplicationFault {
		deleteCode((TYPE_KEY + tenant));
	}		
	
	public String toXml(Map<String, Byte> codes) {
		return super.toXml(codes, "doc");
	}	

}
