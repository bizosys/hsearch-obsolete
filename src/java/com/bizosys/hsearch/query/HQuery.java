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
package com.bizosys.hsearch.query;

/**
 * The query processing data Object which moves in the pipe.
 * Each pipe consumes from this value object and optionally 
 * adds further information for processing by down pipes.
 * @author karan
 *
 */
public class HQuery {

	/**
	 * The user query context.
	 */
	public QueryContext ctx;
	
	/**
	 * Computed query execution planning
	 */
	public QueryPlanner planner;
	
	/**
	 * Query processing result
	 */
	public QueryResult result =null;
	
	/**
	 * Constructor
	 * @param ctx
	 * @param planner
	 */
	public HQuery(QueryContext ctx, QueryPlanner planner ) {
		this.ctx = ctx;
		this.planner = planner;
		this.result = new QueryResult();
	}
	
}
