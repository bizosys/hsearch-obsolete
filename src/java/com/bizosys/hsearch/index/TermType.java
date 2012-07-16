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
 * Defines various term types.
 * To save indexing space, each term type is stored as a byte code.
 * So totally 256 diffierent codes can be defined in the existing index.  
 * @author karan
 *
 */
public class TermType extends TypeCode {
	
	public static String TYPE_KEY = "TERM_TYPE";
	public static final String NONE = "NONE";
	public static final Byte NONE_TYPECODE = Byte.MIN_VALUE;
	public static final String KEYWORD = "KEYWORD";
	public static final String BODY = "BODY";
	public static final String TITLE = "TITLE";
	public static final String URL_OR_ID = "URL";
	
	public static TermType instance = null;
	public static TermType getInstance(boolean concurrency) throws SystemFault {
		if ( null != instance ) return instance;
		synchronized (TermType.class) {
			if ( null != instance ) return instance;
			instance = new TermType();
			return instance;
		}
	}
	
	public void append(String tenant, String type, Byte code) 
	throws ApplicationFault, SystemFault {
		Map<String, Byte> types = this.load(tenant);
		if ( null == types) types = new HashMap<String, Byte>(1);
		types.put(type, code);
		persist(tenant, types);
	}
	
	public void append(String tenant, Map<String, Byte> types) throws SystemFault, ApplicationFault {
		Map<String, Byte> codes = load(tenant);
		if ( null == codes) codes = types;
		else codes.putAll(types);
		persist(tenant, codes);
	}	
	
	@Override
	public void persist(String tenant, Map<String, Byte> types) throws SystemFault {

		byte[] key = (TYPE_KEY + tenant).getBytes();
		super.persist(tenant, types, key);
	}

	@Override
	public Map<String, Byte> load(String tenant) throws SystemFault, ApplicationFault {
		byte[] key = (TYPE_KEY + tenant).getBytes();
		return super.load(tenant, key);
	}
	
	@Override
	public void truncate(String tenant) throws SystemFault, ApplicationFault {
		deleteCode((TYPE_KEY + tenant));
	}		
	
	
	@Override 
	public Map<String, Byte> getDefaultCodes() throws SystemFault, ApplicationFault {
		Map<String, Byte> codes = new HashMap<String, Byte>(256);
		
		byte pos = Byte.MIN_VALUE;
		codes.put(KEYWORD, pos);
		pos++;
		codes.put(BODY, pos);
		
		pos++;
		codes.put(TITLE, pos);
		
		pos++;
		codes.put(URL_OR_ID, pos);
		
		return codes;
	}
	
	
	public String toXml() {
		if ( null == tenantTypeCodes) return "<termtypes></termtypes>";
		
		StringBuilder sb = new StringBuilder();
		sb.append("<termtypes>");
		for (String tenant : tenantTypeCodes.keySet()) {
			sb.append("<tenant><name>").append(tenant).append("</name>");
			Map<String, Byte> codes = tenantTypeCodes.get(tenant);
			for (String termType: codes.keySet()) {
				sb.append("<term>");
				sb.append("<type>").append(termType).append("</type>");
				sb.append("<code>").append( (int) codes.get(termType)).append("</code>");
				sb.append("</term>");
			}
			sb.append("</tenant>");
		}
		sb.append("</termtypes>");
		return sb.toString();
	}
	
	public String toXml(Map<String, Byte> codes) {
		return super.toXml(codes, "term");
	}		
}
