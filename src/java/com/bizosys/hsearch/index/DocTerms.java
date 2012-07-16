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

import java.util.List;

import com.bizosys.hsearch.util.ObjectFactory;

/**
 * This is the term vector specific to a document.
 * @author karan
 */
public class DocTerms {

	private List<TermStream> tokenStreams = null;
	public List<Term> all = null;

	public List<Term> getTermList() {
		if ( null != all) return all;
		all = ObjectFactory.getInstance().getTermList();
		return all;
	}
	
	public void closeTermList() {
		if ( null == all) return;
		ObjectFactory.getInstance().putTermList(all);
	}

	public void setDocumentTypeCode(byte code) {
		if ( null == all) return;
		for (Term term : all) {
			term.setDocumentTypeCode(code);
		}
	}
	
	public List<TermStream> getTokenStreams() {
		return this.tokenStreams;
	}

	public void closeTokenStreams() {
		if ( null == this.tokenStreams) return;
		ObjectFactory.getInstance().putStreamList(this.tokenStreams);
	}

	/**
	 * At which location this stream was found..
	 * @param stream	The token stream
	 */
	public void addTokenStream(TermStream stream) {
		if ( IndexLog.l.isDebugEnabled())
			IndexLog.l.debug("DocTerms > Adding token stream - " + stream.sighting + " - " + stream.type);
		if ( null == tokenStreams) 
			tokenStreams = ObjectFactory.getInstance().getStreamList();
		
		tokenStreams.add(stream);		
	}

	/**
	 * Release all the held resources. Recycles this object for the
	 * next processing.
	 */
	public void cleanup() {
		closeTermList();
		try { 
			if ( null != this.tokenStreams) {
				for (TermStream tstream: this.tokenStreams) {
					tstream.stream.close();
				}
				ObjectFactory.getInstance().putStreamList(this.tokenStreams);
				this.tokenStreams = null;
			}
		} catch (Exception ex) {
			IndexLog.l.warn("DocTerms:cleanup: ", ex);
		}
	}
}
