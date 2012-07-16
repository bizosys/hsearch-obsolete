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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.ParallelHTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.index.DocumentType;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.hsearch.query.QueryLog;
import com.bizosys.hsearch.query.QueryTerm;
import com.bizosys.oneline.services.async.AsyncProcessor;

/**
 * This implements callable interface for execution in parallel
 * This actually executes and fetches IDs from the HBase table.
 * @author karan
 *
 */
class SequenceProcessorFindHBaseParallel extends SequenceProcessorFindHBase {
	
	public SequenceProcessorFindHBaseParallel(QueryTerm term, List<byte[]> findWithinBuckets) {
		super(term,findWithinBuckets);
	}
	
	/**
	 * Go to respective table, colFamily, call
	 * Pass the Matching IDs, Term Type, Document Type, Security Information
	 * Collect only matching Document Sequences 
	 */
	public Object call() throws Exception {
		
		QueryLog.l.debug("SequenceProcessorFindHBaseParallel > Call START");
		if ( null == super.term) return null;
		
		/**
		 * Step 1 Identify table, family and column
		 */
		char tableName = super.term.lang.getTableName(this.term.wordStemmed);
		char familyName = super.term.lang.getColumnFamily(this.term.wordStemmed);
		char colName = super.term.lang.getColumn(this.term.wordStemmed);
		
		/**
		 * Step 2 Configure Filtering mechanism 
		 */
		ParallelHTable table = null;
		ResultScanner scanner = null;

		try {

			table = new ParallelHTable(
					HBaseFacade.getInstance().getHBaseConfig(),
					new byte[] {((byte)tableName)}, AsyncProcessor.getInstance().getThreadPool());
			
			/**
			 * Configure the scanning mechanism.
			 */
			byte[] familyB = new byte[]{(byte)familyName};
			byte[] nameB = new byte[]{(byte)colName};
			Scan scan = configScanner(familyB, nameB);
			scanner = table.getScanner(scan);
			
			byte[] storedB = null;
			byte[] row = null;
			boolean hasTypeFilter = ( DocumentType.NONE_TYPECODE != term.docTypeCode 
				|| TermType.NONE_TYPECODE != term.termTypeCode);
			Set<Integer> ignorePos = (hasTypeFilter) ? new HashSet<Integer>() :null;
			
			readScanner(scanner, familyB, nameB, storedB, row, hasTypeFilter, ignorePos);
			
			
		} catch ( IOException ex) {
			QueryLog.l.fatal("SequenceProcessorFindHBase:", ex);
			return null;
		} finally {
			if ( null != scanner) scanner.close();
		}		
		return null;
	}
}