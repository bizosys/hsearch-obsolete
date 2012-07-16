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

import com.bizosys.hsearch.index.Doc;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.index.IdMapping;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;

/**
 * Delete an existing record before duplicating it.
 * @author karan
 *
 */
public class FilterDuplicateId implements PipeIn {
	
	public FilterDuplicateId() {}

	public void commit(boolean concurrency) throws ApplicationFault, SystemFault {
	}

	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "FilterDuplicateId";
	}

	public void init(Configuration conf) {
	}
 
	public void visit(Object docObj, boolean multiWriter) throws ApplicationFault, SystemFault {
		if ( null == docObj) throw new ApplicationFault("No document");
		Doc doc = (Doc) docObj;
		DocTeaser teaser = doc.teaser;
		if ( null == teaser) throw new ApplicationFault("No Teaser");
		if ( null == teaser.id) throw new ApplicationFault("No Id");
		
		IdMapping mapping = IdMapping.load(doc.tenant, teaser.id);
		if ( null == mapping ) {
			if ( InpipeLog.l.isDebugEnabled() ) InpipeLog.l.debug(
				"FilterDuplicateId: Skipping delete - " + doc.tenant + "-" + teaser.id);
			return;
		}
		Doc docDelete = new Doc(mapping.bucketId,mapping.docSerialId, true);
		if ( null == docDelete.teaser ) return;
		
		IndexWriter.getInstance().delete(doc.tenant,true, docDelete);
	}
	
}
