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
package com.bizosys.hsearch.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

/**
 *	Url shortner reads urlmappings file and applies shortening <br />
 *  Example (The URL and tab separated short code) <br />
 *  http://www.bizosys.com/employee.xml/id=	01 <br />
 *  http://www.bizosys.com/employee?	02 <br />
 *  http://www.bizosys.com/company.xml/id=	03 <br />
 *  
 * @author Abinasha Karana
 */
public class UrlShortner {

	private static UrlShortner instance = null;
	
	public static final UrlShortner getInstance() throws ApplicationFault, SystemFault{
		if ( null != instance) return instance;
		synchronized (UrlShortner.class) {
			if ( null != instance ) return instance;
			instance = new UrlShortner();
			instance.refresh();
		}
		return instance;
	}
	
	public HashMap<String, String> urls = null;
	public HashMap<String, String> codes = null;
	private byte[] SHORTNER_KEY =  "urlshortner".getBytes();
	
	private UrlShortner() {
	}
	
	public void refresh() throws ApplicationFault, SystemFault{
		if ( null != urls) urls.clear();
		if ( null != codes) codes.clear();

		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES);
		try {
			RecordScalar scalar = new RecordScalar(SHORTNER_KEY, nv);
			HReader.getScalar(IOConstants.TABLE_CONFIG, scalar);
		} catch (Exception ex) {
			UtilLog.l.warn("UrlMapper: Could not read urlmappings configurations.", ex);
		}
		
		if ( null == nv.data) return;
		byte[] bytes = nv.data.toBytes();
		int bytesT = bytes.length;
		int start=0;
		
		List<String> lstUrl = new ArrayList<String>();
		List<String> lstCode = new ArrayList<String>();
		while ( ( start + 1 ) < bytesT ) {
			start = cut(bytes, bytesT, start, '\t', lstUrl);
			start = cut(bytes, bytesT, start, '\n', lstCode);
		}

		int urlT = lstUrl.size();
		if ( urlT != lstCode.size()) 
			throw new ApplicationFault("Wrong configuration file.");
		
		if ( urlT == 0 ) return;
				
		urls = new HashMap<String, String>(urlT);
		codes = new HashMap<String, String>(urlT);
		for (int i=0; i< urlT; i++) {
			urls.put(lstUrl.get(i) , lstCode.get(i));
			codes.put(lstCode.get(i), lstUrl.get(i));
		}
		
		lstCode.clear();
		lstUrl.clear();
	}
	
	public void persist(Map<String, String> values) throws ApplicationFault, SystemFault{
		StringBuilder sb = new StringBuilder();
		for (String url : values.keySet()) {
			sb.append(url).append('\t').append(values.get(url)).append('\n');
		}
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES);
		nv.data = new Storable(sb.toString());
		try {
			RecordScalar scalar = new RecordScalar(SHORTNER_KEY, nv);
			HWriter.getInstance(false).insertScalar(IOConstants.TABLE_CONFIG, scalar);
			if ( UtilLog.l.isDebugEnabled() ) 
				UtilLog.l.debug("Url Mapping is saved sucessfully");
		} catch (Exception ex) {
			UtilLog.l.warn("UrlMapper: Could not save urlmappings configurations.", ex);
		}
	}	
	
	private int cut(byte[] bytes, int bytesT, int start, char sep, List<String> list) {
		for( int i=start; i< bytesT; i++) {
			if ( bytes[i] != sep) continue;
			int len = i - start;
			if ( len <= 0 ) continue;
			byte[] urlB = new byte[i - start];
			System.arraycopy(bytes, start, urlB, 0, len);
			list.add(new String(urlB).trim());
			return i;
		}
		return start;
	}
	
	/**
	 * This encodes to the short form of the URL prefix
	 * @param url	URL
	 * @return	Encoded Url
	 */
	public String encoding(String url) {
		
		if ( StringUtils.isEmpty(url)) return null;
		if ( null == urls) return null;
		
		if ( UtilLog.l.isDebugEnabled()) 
			UtilLog.l.debug("UrlShortner encoding:" + url);

		if ( urls != null && urls.containsKey(url) ) return urls.get(url);
		int lastEqualto = url.lastIndexOf('=');
		if ( -1 != lastEqualto) {
			lastEqualto = lastEqualto + 1;
			String prefix = url.substring(0,lastEqualto);
			if ( urls.containsKey(prefix) ) 
				return urls.get(prefix) + '~' + url.substring(lastEqualto) ;
		}
		
		//Can I get an exact till the last / character.
		int lastSlash = url.lastIndexOf('?');
		if ( -1 != lastSlash) {
			String prefix = url.substring(0,lastSlash );
			if ( urls.containsKey(prefix) ) 
				return urls.get(prefix) + '~' +url.substring(lastSlash) ;
		}
		return url;
	}
	
	/**
	 * This decodes the short form of the URL prefix
	 * @param codedUrl	coded URL
	 * @return	The decoded url
	 */
	public String decoding(String codedUrl) {
		if ( StringUtils.isEmpty(codedUrl)) return null;
		if ( null == codes) return null;
		
		if ( UtilLog.l.isDebugEnabled()) 
			UtilLog.l.debug("UrlShortner decoding:" + codedUrl);
		
		//Is there a direct match
		if ( codes.containsKey(codedUrl) ) return codes.get(codedUrl);
		int division = codedUrl.lastIndexOf('~');
		if ( -1 == division) return codedUrl;
		String code = codedUrl.substring(0,division );
		
		if ( codes.containsKey(code) ) 
			return codes.get(code) + codedUrl.substring(division + 1) ;
		
		return codedUrl;
	}
}
