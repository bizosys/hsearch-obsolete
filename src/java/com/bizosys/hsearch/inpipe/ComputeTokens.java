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

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTerms;
import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.Term;
import com.bizosys.hsearch.index.TermStream;
import com.bizosys.hsearch.inpipe.util.ReaderType;

/**
 * Tokenize text content
 * @author karan
 *
 */
public class ComputeTokens implements PipeIn {

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "ComputeTokens";
	}

	public void init(Configuration conf) {
	}

	public void visit(Object docObj, boolean multiWriter) throws SystemFault, ApplicationFault {
		
		if ( null == docObj) throw new ApplicationFault("Null Document");
		Doc doc = (Doc) docObj;
		
		List<TermStream> streams = doc.terms.getTokenStreams();
		if ( null != streams) {
			try {
				for (TermStream stream : streams) {
					tokenize(doc, stream);
					stream.stream.close();
					stream = null;
				}
			} catch (IOException ex) {
				throw new SystemFault ("ComputeTokens : Tokenize Failed." , ex);
			} finally {
				streams.clear();
				cleanReaders(doc);
			}
		}
		
		/**
		 * Assign the document type to all the terms
		 */
		Byte docTypeCode = null;
		if ( null != doc.meta.docType) 
			docTypeCode = DocumentType.getInstance().getTypeCode(doc.tenant, doc.meta.docType);
		if (null == docTypeCode) return;
		doc.terms.setDocumentTypeCode(docTypeCode);
	}
	
	private void tokenize(Doc doc, TermStream ts) throws SystemFault, ApplicationFault, IOException {
		if ( null == ts) return;
		TokenStream stream = ts.stream;
		if ( null == stream) return;
		
		DocTerms terms = doc.terms;
		if ( null == doc.terms) {
			terms = new DocTerms();
			doc.terms = terms; 
		}
		
		String token = null;
		int offset = 0;
		CharTermAttribute termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
		OffsetAttribute offsetA = (OffsetAttribute) stream.getAttribute(OffsetAttribute.class);
		stream.reset();
		while ( stream.incrementToken()) {
			token = termA.toString();
			offset = offsetA.startOffset();
			Term term = new Term(doc.tenant, token,ts.sighting,ts.type,offset);
			terms.getTermList().add(term);
		}
		stream.close();
	}
	
	public void commit(boolean arg0){
	}
	
	private void cleanReaders(Doc doc) {
		if ( null == doc.readers) return;
		for (ReaderType reader : doc.readers) {
			try {
				if ( null != reader.reader ) reader.reader.close();
			} catch (IOException ex) {
				InpipeLog.l.warn("ComputeTokens:cleanReaders() reader closing failed", ex);
			}
		}
		doc.readers.clear();
		doc.readers = null;
	}
	
}
