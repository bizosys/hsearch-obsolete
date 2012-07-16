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
package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.oneline.SystemFault;

public class HDML {
    private static final boolean INFO_ENABLED = HbaseLog.l.isInfoEnabled();
	private static final boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();

	/**
	 * Creates the table if not existing before
	 * @param tableName
	 * @param cols
	 * @throws IOException
	 */
    public static final boolean create(String tableName, List<HColumnDescriptor> cols) 
    throws SystemFault {
    	
		if  (DEBUG_ENABLED) 
			HbaseLog.l.debug("Creating HBase Table - " + tableName);
		
		try {
			if  (DEBUG_ENABLED) 
				HbaseLog.l.debug("Checking for table existance : " + tableName);
			HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
        	if ( admin.tableExists(tableName)) {

        		if  (INFO_ENABLED) 
	        		HbaseLog.l.info("Ignoring creation. Table already exists - " + tableName);
        		return false;
        	} else {
        		HTableDescriptor tableMeta = new HTableDescriptor(tableName);
        		for (HColumnDescriptor col : cols) tableMeta.addFamily(col);
        		admin.createTable(tableMeta);
        		if  (INFO_ENABLED ) HbaseLog.l.info("Table Created - " + tableName);
        		return true;
        	}

		} catch (TableExistsException ex) {
			HbaseLog.l.warn("Ignoring creation. Table already exists - " + tableName, ex);
			throw new SystemFault("Failed Table Creation : " + tableName, ex);
		} catch (MasterNotRunningException mnre) {
			throw new SystemFault("Failed Table Creation : " + tableName, mnre);
		} catch (IOException ioex) {
			throw new SystemFault("Failed Table Creation : " + tableName, ioex);
		}
	}
    
    
	/**
	 * Drop a table. This may take significantly large time as things
	 * are disabled first and then gets deleted. 
	 * @param tableName
	 * @throws IOException
	 */
	public static void drop(String tableName) throws SystemFault{

		if  (DEBUG_ENABLED) 
			HbaseLog.l.debug("Checking for table existance");
		
		try {
			HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
	    	byte[] bytesTableName = tableName.getBytes();
			if ( admin.tableExists(bytesTableName)) {
	    		if ( ! admin.isTableDisabled(bytesTableName) ) 
	    			admin.disableTable(bytesTableName);
	    		if ( admin.isTableDisabled(bytesTableName) ) 
	    				admin.deleteTable(bytesTableName);
				if  (INFO_ENABLED ) HbaseLog.l.info (tableName + " Table is deleted.");
	    	} else {
	    		HbaseLog.l.warn( tableName + " table is not found during drop operation.");
	    		throw new SystemFault("Table does not exist");
	    	}
		} catch (IOException ioex) {
			throw new SystemFault("Table Drop Failed : " + tableName, ioex);
		}
	}
	
	public static void truncate(String tableName, NV kv) throws IOException {
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> matched = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(kv.family.toBytes(), kv.name.toBytes());
			scanner = table.getScanner(scan);
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				Delete delete = new Delete(r.getRow());
				delete = delete.deleteColumns(kv.family.toBytes(), kv.name.toBytes());
				table.delete(delete);
			}
		} finally {
			table.flushCommits();
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
			if ( null != matched) matched.clear();
		}
	}		
	
	public static void truncateBatch(String tableName, String keyPrefix) throws IOException {
		
		if  (INFO_ENABLED) HbaseLog.l.info(
			"Deleted from " + tableName + " with prefix " + keyPrefix);

		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<Delete> deletes = new ArrayList<Delete>(256);
		
		int batchSize = 0;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			if ( null != keyPrefix) {
				Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryPrefixComparator(Storable.putString(keyPrefix)));
				scan = scan.setFilter(rowFilter);
			}
			scanner = table.getScanner(scan);
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				Delete delete = new Delete(r.getRow());
				deletes.add(delete);
				
				batchSize++;
				if ( batchSize > 1000) {
					if ( deletes.size() > 0 ) {
						table.delete(deletes);
						deletes.clear();
					}
					batchSize = 0;
				}
			}
			if ( deletes.size() > 0 ) table.delete(deletes);
			
		} finally {
			table.flushCommits();
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
			if ( null != deletes) deletes.clear();
		}
	}			
	
	public static void truncateBatch(String tableName, List<byte[]> rows) throws IOException {
		
		if ( null == rows) return;
		if ( rows.size() == 0) return;
		
		HBaseFacade facade = null;
		HTableWrapper table = null;
		List<Delete> deletes = new ArrayList<Delete>(rows.size());
		
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			for (byte[] row : rows) {
				Delete delete = new Delete(row);
				deletes.add(delete);
			}
			table.delete(deletes);
			
		} finally {
			table.flushCommits();
			if ( null != table ) facade.putTable(table);
			if ( null != deletes) deletes.clear();
		}
	}			
}
