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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTeaser;

/**
 * Condense multiple blanks to a single blank. This helps in 
 * forming cache text and eventually result display.
 * @author karan
 *
 */
public class RemoveBlankSpace implements PipeIn {

	Pattern pattern = null;
	String replaceStr = " ";
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "RemoveBlankSpace";
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
		this.pattern = Pattern.compile("\\s+");		
	}

	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;
		
    	DocTeaser teaser = doc.teaser;
    	if ( null != teaser) {
    		String titleText = teaser.getTitle();
    		if ( null != titleText) teaser.setTitle(strip(titleText));

    		String cacheText = teaser.getCachedText();
    		if ( null != cacheText) teaser.setCacheText(strip(cacheText));
    	}
	}

	public synchronized String strip ( String text) {
		Matcher matcher = pattern.matcher(text);
		return matcher.replaceAll(replaceStr);
	}	

	public void commit(boolean arg0) throws ApplicationFault, SystemFault {
	}
	
}
