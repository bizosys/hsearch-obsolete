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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.bizosys.oneline.SystemFault;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.util.RecordScalar;

public class HReader {
	
	/**
	 * Scalar data will contain the amount to increase
	 * @param tableName
	 * @param scalar
	 * @throws SystemFault
	 */
	public static long idGenerationByAutoIncr(String tableName, 
		RecordScalar scalar, long amount ) throws SystemFault {
		
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			long incrementedValue = table.incrementColumnValue(
					scalar.pk.toBytes(), scalar.kv.family.toBytes(), 
					scalar.kv.name.toBytes(), amount);
			return incrementedValue;
		} catch (Exception ex) {
			throw new SystemFault("Error in getScalar :" + scalar.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static boolean exists (String tableName, byte[] pk) throws SystemFault{
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(pk);
			return table.exists(getter);
		} catch (Exception ex) {
			throw new SystemFault("Error in existance checking :" + pk.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}

	public static List<NVBytes> getCompleteRow (String tableName, 
		byte[] pk) throws SystemFault{
		
		return getCompleteRow (tableName, pk, null, null);
	}
	
	public static List<NVBytes> getCompleteRow (String tableName, byte[] pk, 
		Filter filter) throws SystemFault {
		
		return getCompleteRow (tableName, pk, filter, null);
	}		
	public static List<NVBytes> getCompleteRow (String tableName, byte[] pk, 
		Filter filter, RowLock lock) throws SystemFault {
		
		HBaseFacade facade = null;
		HTableWrapper table = null;
		Result r = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = ( null == lock) ? new Get(pk) : new Get(pk,lock);  
			if  (null != filter) getter.setFilter(filter);
			if ( table.exists(getter) ) {
				r = table.get(getter);
				if ( null == r ) return null;
				List<NVBytes> nvs = new ArrayList<NVBytes>(r.list().size());
				for (KeyValue kv : r.list()) {
					NVBytes nv = new NVBytes(kv.getFamily(),kv.getQualifier(), kv.getValue());
					nvs.add(nv);
				}
				return nvs;
			}
			return null;
		} catch (Exception ex) {
			throw new SystemFault("Error in existance checking :" + pk.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}	
	
	public static void getScalar (String tableName, RecordScalar scalar) throws SystemFault {
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(scalar.pk.toBytes());
			Result result = table.get(getter);
			if ( null == result) return;
			byte[] val = result.getValue(scalar.kv.family.toBytes(), scalar.kv.name.toBytes());
			if ( null != val ) scalar.kv.data = new Storable(val); 
		} catch (Exception ex) {
			throw new SystemFault("Error in getScalar :" + scalar.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static byte[] getScalar (String tableName, 
		byte[] family, byte[] col, byte[] pk) throws SystemFault{

		return getScalar(tableName,family,col,pk,null);		
	}	
		
	
	public static byte[] getScalar (String tableName, 
			byte[] family, byte[] col, byte[] pk, Filter filter) throws SystemFault{
		
		if ( null == family || null == col || null == pk ) return null;
		
		HBaseFacade facade = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			Get getter = new Get(pk);
			if ( null != filter) getter = getter.setFilter(filter);
			Result result = table.get(getter);
			if ( null == result) return null;
			return result.getValue(family, col);
		} catch (Exception ex) {
			StringBuilder sb = new StringBuilder();
			sb.append("Input during exception = Table : [").append(tableName);
			sb.append("] , Family : [").append(Storable.getString(family));
			sb.append("] , Column : [").append(Storable.getString(col));
			sb.append("] , Key : [").append(Storable.getString(pk));
			sb.append(']');
			throw new SystemFault(sb.toString(), ex);
		} finally {
			if ( null != facade && null != table) facade.putTable(table);
		}
	}
	
	public static void getAllValues(String tableName, NV kv, 
		String keyPrefix, IScanCallBack callback ) throws SystemFault {
		
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
			
			if ( null != keyPrefix) {
				Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryPrefixComparator(Storable.putString(keyPrefix)));
				scan = scan.setFilter(rowFilter);
			}
			
			scanner = table.getScanner(scan);
			
			long timeS = System.currentTimeMillis();
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				byte[] storedBytes = r.getValue(kv.family.toBytes(), kv.name.toBytes());
				if ( null == storedBytes) continue;
				callback.process(storedBytes);
			}
			if ( HbaseLog.l.isDebugEnabled()) {
				long timeE = System.currentTimeMillis();
				HbaseLog.l.debug("HReader.getAllValues (" + tableName + ") execution time = " + 
					(timeE - timeS) );
			}
			
		} catch ( IOException ex) {
			throw new SystemFault(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
			if ( null != matched) matched.clear();
		}
	}
	
	/**
	 * Get all the keys of the table cutting the keyPrefix.
	 * @param tableName	Table name
	 * @param kv	Key-Value
	 * @param startKey	Start Row Primary Key
	 * @param pageSize	Page size
	 * @return	Record Keys	
	 * @throws SystemFault
	 */
	public static void getAllKeys(String tableName, NV kv, 
			String keyPrefix, IScanCallBack callback) throws SystemFault {
	
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;

		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(kv.family.toBytes(), kv.name.toBytes());

			if ( null != keyPrefix) {
				Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryPrefixComparator(Storable.putString(keyPrefix)));
				scan = scan.setFilter(rowFilter);
			}
			
			scanner = table.getScanner(scan);
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				callback.process(r.getRow());
			}
		} catch ( IOException ex) {
			throw new SystemFault(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}
	
	/**
	 * Get the keys of the table
	 * @param tableName	Table name
	 * @param kv	Key-Value
	 * @param startKey	Start Row Primary Key
	 * @param pageSize	Page size
	 * @return	Record Keys	
	 * @throws SystemFault
	 */
	public static List<byte[]> getKeysForAPage(String tableName, NV kv, 
		byte[] startKey, String keyPrefix, int pageSize) throws SystemFault {
	
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> keys = ( pageSize > 0 ) ?
			new ArrayList<byte[]>(pageSize): new ArrayList<byte[]>(1024);
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addColumn(kv.family.toBytes(), kv.name.toBytes());

			if( null != keyPrefix) {
				Filter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(keyPrefix)));
				scan = scan.setFilter(rowFilter);
			}
			
			if ( pageSize > 0) {
				PageFilter pageFilter = new PageFilter(pageSize);
				scan = scan.setFilter(pageFilter);
			}
			
			if ( null != startKey) scan = scan.setStartRow(startKey); 

			scanner = table.getScanner(scan);
			
			int counter = 0;
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				if ( counter++ > pageSize) break;
				keys.add(r.getRow());
			}
			return keys;
		} catch ( IOException ex) {
			throw new SystemFault(ex);
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}		
}
