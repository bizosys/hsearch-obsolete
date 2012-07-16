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
package com.bizosys.hsearch.outpipe;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.filter.TeaserFilterSingular;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.query.DocMetaWeight;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.QueryLog;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

/**
 * This fetches teasers from HBase. Each record represents one 
 * teaser.
 * @author karan
 *
 */
class BuildPreviewSingular {
	
	private TeaserFilterSingular pf = null;
	
	protected BuildPreviewSingular(byte[][] wordsB, short teaserSize) {
		this.pf = new TeaserFilterSingular(wordsB, teaserSize);
	}
	
	protected List<DocTeaserWeight> filter(Object[] metaL, int pageSize )
		throws SystemFault, ApplicationFault {
		
		QueryLog.l.debug("TeaserFilterSingular > Start");
		if ( null == this.pf) return null;
		
		/**
		 * Bring the pointer to beginning from the end
		 */
		int metaT = metaL.length;
		if ( pageSize > metaT) pageSize = metaT;
		
		List<DocTeaserWeight> foundDocs = new ArrayList<DocTeaserWeight>();
		for ( int i=0; i< pageSize; i++) {
			DocMetaWeight metaWt =  (DocMetaWeight) metaL[i]; 
			byte[] idB = metaWt.getIdBytes();
			byte[] data = HReader.getScalar(IOConstants.TABLE_PREVIEW,
					IOConstants.TEASER_BYTES,IOConstants.TEASER_BYTES,
					idB,this.pf);
			
			if ( null == data) continue;
			foundDocs.add(new DocTeaserWeight(idB,data,metaWt.weight));
		}
		if ( QueryLog.l.isInfoEnabled() )
			QueryLog.l.info("BuildTeaserSingular > Found Documents:" + foundDocs.size());
		return foundDocs;
	}
}