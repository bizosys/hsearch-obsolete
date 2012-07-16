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

import java.util.List;

import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeOut;
import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.query.HQuery;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryPlanner;
import com.bizosys.hsearch.query.QueryTerm;

/**
 * Resolve the term type and document type code.
 * @see TermType
 * @see DocumentType
 * @author karan
 *
 */
public class ComputeTypeCodes implements PipeOut{
	
	public ComputeTypeCodes() {
	}	

	public void visit(Object objQuery, boolean multiWriter) throws ApplicationFault, SystemFault {

		HQuery query = (HQuery) objQuery;
		QueryContext ctx = query.ctx;
		QueryPlanner planner = query.planner;
		
		this.deduceDocumentType(ctx, multiWriter);
		this.deduceTermType(ctx, planner.mustTerms, multiWriter);
		this.deduceTermType(ctx, planner.optionalTerms, multiWriter);
	}
	
	private void deduceDocumentType(QueryContext ctx, boolean multiWriter )
	throws SystemFault,ApplicationFault{
		
		if ( null == ctx) return ;
		if ( null == ctx.docType) return ;
		ctx.docTypeCode = DocumentType.getInstance().getTypeCode(
			ctx.getTenant(), ctx.docType);
	}
	
	private void deduceTermType(QueryContext ctx, List<QueryTerm> queryWordL, 
		boolean multiWriter ) throws SystemFault,ApplicationFault {
		
		if ( null == queryWordL) return;
		String tenant = ctx.getTenant();
		for (QueryTerm term : queryWordL) {
			if ( StringUtils.isEmpty(term.termType) ) continue;
			term.termTypeCode = TermType.getInstance(multiWriter). 
				getTypeCode(tenant, term.termType);
		}
	}
	
	public boolean commit() throws ApplicationFault, SystemFault {
		return true;
	}

	public PipeOut getInstance() {
		return this;
	}

	public void init(Configuration conf) throws ApplicationFault, SystemFault {
	}
	
	public void commit(boolean multiWriter) throws ApplicationFault, SystemFault {
		
	}
	
	public String getName() {
		return "ComputeTypeCodes";
	}	
}
