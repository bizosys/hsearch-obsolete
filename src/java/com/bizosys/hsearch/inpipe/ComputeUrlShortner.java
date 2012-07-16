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

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.util.IpUtil;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Deduce the Host IP address based on URL. 
 * It converts the IP address to a single integer (IP House)
 * @see IpUtil	 
 * @author karan
 *
 */
public class ComputeUrlShortner implements PipeIn {

	public PipeIn getInstance() { 
		return this; 	
	}

	public String getName() { 
		return "ComputeUrlShortner"; 	
	}

	public void init(Configuration conf)  { 
	}

	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault {
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;
		
    	DocMeta meta = doc.meta;
    	if ( null == meta ) throw new ApplicationFault("No Meta");
    	
    	DocTeaser teaser = doc.teaser;
    	if ( null == teaser ) throw new ApplicationFault("No Teasers");

    	if ( null == teaser.url ) return;
    	try {
	    	String url = teaser.url;
	    	String urlL =url.toLowerCase();
	    	if ( ! ( urlL.startsWith("http") || 
	    		urlL.startsWith("ftp") ) )  return; 
	    	URL resolvedUrl = new URL(url); 
	    	InetAddress ipaddress = InetAddress.getByName(resolvedUrl.getHost());
	    	meta.ipHouse = IpUtil.computeHouse(ipaddress.getHostAddress());
	    	return;
    	} catch (UnknownHostException ex) {
    		InpipeLog.l.info(ex);
    		throw new ApplicationFault(ex);
    	} catch (MalformedURLException ex) {
    		InpipeLog.l.info(ex);
    		throw new ApplicationFault(ex);
    	}
	}

	public void commit(boolean arg0) throws ApplicationFault, SystemFault {
	}
	
}
