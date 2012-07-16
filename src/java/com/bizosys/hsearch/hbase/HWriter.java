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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.util.ObjectFactory;
import com.bizosys.hsearch.util.Record;
import com.bizosys.hsearch.util.RecordScalar;

/**
 * All HBase write calls goes from here.
 * It supports Insert, Delete, Update and Merge operations. 
 * Merge is a operation, where read and write happens inside 
 * a lock. This lock is never exposed to caller function.
 * @author karan
 *
 */
public class HWriter {
	
	private static final boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	//private boolean isBatchMode = false;
	private static HWriter singleton = null; 
	
	/**
	 * Factory for getting HWriter instance.
	 * Currently HWriter can execute in a thread safe environment with 
	 * multiple writers originating from a singel machine or multi  
	 * machine environment or out of a single thread write environment. 
	 * @param enableThreadSafety	Should it run in a parallel clients mode
	 * @return	HWriter instance.
	 */
	public static HWriter getInstance(boolean enableThreadSafety ) {
		if ( null != singleton) return singleton;
		synchronized (HWriter.class) {
			if ( null != singleton) return singleton;
			singleton = new HWriter(); 
		}
		return singleton;
	}
	
	/**
	 * Default constructor.
	 * Don't use
	 */
	private HWriter() {
	}

	/**
	 * Insert just a single scalar record. If the record is already existing, it overrides.
	 * A scalar record contains just one column.
	 * @param tableName	Table name
	 * @param record	A Table record
	 * @throws IOException
	 */
	public void insertScalar(String tableName, RecordScalar record) throws IOException {
		if  (DEBUG_ENABLED)  HbaseLog.l.debug("HWriter> insertScalar:record " + tableName);
		
		byte[] pk = record.pk.toBytes();
		Put update = new Put(pk);
		NV kv = record.kv;
		update.add(kv.family.toBytes(),kv.name.toBytes(), kv.data.toBytes());
   		update.setWriteToWAL(true);
		
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
	
	/**
	 * Insert multiple scalar records. If records exist, it overrides
	 * A scalar record contains just one column.
	 * @param tableName	Table name
	 * @param records	Table records
	 * @throws IOException
	 */
	public void insertScalar(String tableName, 
			List<RecordScalar> records) throws IOException {
		
		if  (DEBUG_ENABLED) HbaseLog.l.debug("HWriter> insertScalar:records table " + tableName);
		
		List<Put> updates = ObjectFactory.getInstance().getPutList();
		
		for (RecordScalar record : records) {
			Put update = new Put(record.pk.toBytes());
			NV kv = record.kv;
			update.add(kv.family.toBytes(),kv.name.toBytes(), kv.data.toBytes());
			update.setWriteToWAL(true);
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(updates);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
			if ( null != updates) ObjectFactory.getInstance().putPutsList(updates);
		}
	}
	
	/**
	 * Insert a record
	 * @param tableName
	 * @param record
	 * @throws IOException
	 */
	public void insert(String tableName, Record record) throws IOException {
		if  (DEBUG_ENABLED) HbaseLog.l.debug("HWriter> insert to table " + tableName);
		
   		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			Put update = new Put(record.pk.toBytes());
	   		for (NV param : record.getNVs()) {
				update.add(param.family.toBytes(), 
					param.name.toBytes(), param.data.toBytes());
			}
	   		update.setWriteToWAL(true);
	   		facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			table.put(update);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}	
	
	/**
	 * Inserting multiple records. It overrides the values of existing records.
	 * from the time we have read..
	 * @param tableName
	 * @param records
	 * @throws IOException
	 */
	public void insert(String tableName, List<Record> records) throws IOException {
		if  (DEBUG_ENABLED) HbaseLog.l.debug("HWriter> insert:records to table " + tableName);
		
		List<Put> updates = ObjectFactory.getInstance().getPutList();
		
		for (Record record : records) {
			Put update = new Put(record.pk.toBytes());
	   		for (NV param : record.getNVs()) {
				update.add(param.family.toBytes(), 
					param.name.toBytes(), param.data.toBytes());
			}
	   		update.setWriteToWAL(true);
			updates.add(update);
		}
		HTableWrapper table = null;
		HBaseFacade facade = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			if  (DEBUG_ENABLED)  HbaseLog.l.debug("HWriter> insert:Putting records " + updates.size());
			table.put(updates);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
			if ( null != updates) ObjectFactory.getInstance().putPutsList(updates);
		}
	}
	
	/**
	 * Update a table. It calls back the update call back function for
	 * various modifications during update operations as bytes merging.
	 * @param tableName
	 * @param pk
	 * @param pipe
	 * @param families
	 * @throws IOException
	 */
	public void update(String tableName, 
		byte[] pk, IUpdatePipe pipe, byte[][] families) throws IOException {
		
		if ( null == tableName  || null == pk) return;
		if  (DEBUG_ENABLED) HbaseLog.l.debug("HWriter> update to table " + tableName);

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		RowLock lock = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			/**
			 * Scope down the existance check getter, not to mingle with actual one.
			 */
			if ( 1 == 1) { 
				Get existanceGet = new Get(pk);
				if ( ! table.exists(existanceGet) ) return;
			}

			lock = table.lockRow(pk);
			Get lockedGet  =  ( null == lock) ? new Get(pk) : new Get(pk,lock);
			if ( null != families) {
				for (byte[] family : families) {
					lockedGet = lockedGet.addFamily(family);
				}
			}
			
			Put lockedUpdate = null;
			Delete lockedDelete = null;
			
			int familiesT = ( null == families) ? 0 : families.length;
			int[] familyByteLen = new int[familiesT];
			
			Result r = table.get(lockedGet);
			if ( null == r) return;
			if ( null == r.list()) return;
			
			for (KeyValue kv : r.list()) {
				byte[] curVal = kv.getValue();
				if ( null == curVal) continue;
				if ( 0 == curVal.length) continue;
				byte[] modifiedB = pipe.process(kv.getFamily(), kv.getQualifier(), curVal);
				int modifiedBLen = ( null == modifiedB) ? 0 : modifiedB.length;
				
				/**
				 * Count if family to be chucked out
				 * */
				for (int i=0; i<familiesT; i++) {
					byte[] family = families[i];
					if ( ByteField.compareBytes(kv.getFamily(), family)) {
						familyByteLen[i] = familyByteLen[i] + modifiedBLen;
					}
				}
				 
				
				boolean changedValue = false;
				if ( 0 == modifiedBLen) {
					if ( null == lockedDelete ) lockedDelete =   
						( null == lock) ? new Delete(pk) : new Delete(pk, -1, lock);
					lockedDelete = lockedDelete.deleteColumn(kv.getFamily(), kv.getQualifier());
					continue;
				} 
				
				/**
				 * If changed, perform an update
				 */
				if (curVal.length == modifiedBLen) {
					changedValue = ! ByteField.compareBytes(curVal, modifiedB);
				} else {
					changedValue = true;
				}
				
				if ( changedValue) {
					if ( null == lockedUpdate ) 
						lockedUpdate = ( null == lock) ? new Put(pk) : new Put(pk,lock);
					KeyValue updatedKV = new KeyValue(r.getRow(), 
						kv.getFamily(), kv.getQualifier(),modifiedB);  
					lockedUpdate.add(updatedKV);
				}
			}
			
			/**
			 * Flush all updates.
			 */
			if ( null != lockedUpdate ) {
				lockedUpdate.setWriteToWAL(true);
				table.put(lockedUpdate);
			}
			
			/**
			 * Flush all deletes
			 */
			if ( null != lockedDelete ) {
				for (int i=0; i<familiesT; i++) {
					if ( familyByteLen[i] == 0 ) {
						lockedDelete = lockedDelete.deleteFamily(families[i]);
					}
				}
				table.delete(lockedDelete);
			}
			
			if ( null != lockedUpdate || null != lockedDelete) table.flushCommits();

		} finally {
			boolean goodTable = true;
			try { 
				if ( null != lock ) table.unlockRow(lock); 
			} catch (Exception ex) {
				reportUnlockException(ex);
				goodTable = false;
			}

			if ( null != facade && null != table && goodTable) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * Delete the complete row based on the key
	 * @param tableName	Table name
	 * @param pk	Serialized primary Key
	 * @throws IOException
	 */
	public void delete(String tableName, IStorable pk) throws IOException {
		if ( null == pk) return;
		Delete delete = new Delete(pk.toBytes());

		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	/**
	 * Deletes the supplied columns for the row. 
	 * @param tableName	Table name
	 * @param pk	Storable Primary Key
	 * @param packet	ColumnFamily and ColumnName necessary
	 * @throws IOException
	 */
	public void delete(String tableName, IStorable pk, NV packet) throws IOException {
		
		Delete delete = new Delete(pk.toBytes());
		delete = delete.deleteColumns(packet.family.toBytes(), packet.name.toBytes());
		
		HBaseFacade facade = HBaseFacade.getInstance();
		HTableWrapper table = null;
		try {
			table = facade.getTable(tableName);
			table.delete(delete);
			table.flushCommits();
		} finally {
			if ( null != facade && null != table) {
				facade.putTable(table);
			}
		}
	}
	
	
	/**
	 * Before putting the record, it merges the record.
	 * @param tableName	Table name
	 * @param records	Records
	 * @throws IOException
	 */
	public void mergeScalar(String tableName, List<RecordScalar> records) 
	throws IOException {
			
		if ( null == tableName  || null == records) return;
		if  (DEBUG_ENABLED) 
			HbaseLog.l.debug("HWriter: mergeScalar (" + tableName + ") , Count =" + records.size());

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		List<RowLock> locks = ObjectFactory.getInstance().getRowLockList();
		List<Put> updates = ObjectFactory.getInstance().getPutList();
		
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);

			for (RecordScalar scalar : records) {
				byte[] pk = scalar.pk.toBytes();
				if ( 0 == pk.length) continue;;
				Get getter = new Get(pk);
				byte[] famB = scalar.kv.family.toBytes();
				byte[] nameB = scalar.kv.name.toBytes();
				RowLock lock = null;
				
				if ( table.exists(getter) ) {
					lock = table.lockRow(pk);
					if ( null == lock) {
						throw new IOException("Unable to aquire lock," + new String(pk) + 
							" for the table - " + tableName);
					}
					locks.add(lock);
					
					Get existingGet = (null == lock) ?   new Get(pk) : new Get(pk, lock);
					existingGet = existingGet.addColumn(famB, nameB);
					Result r = table.get(existingGet); 
					if ( ! scalar.merge(r.getValue(famB, nameB)) ) {
						if ( null != lock ) {
							table.unlockRow(lock);
							locks.remove(lock);
						}
						continue;
					}
				}

				NV kv = scalar.kv;
				byte[] data = kv.data.toBytes();
				if ( null == data ) {
					try { 
						if ( null != lock ) {
							table.unlockRow(lock);
							updates.remove(lock);
							lock = null;
						}
						
					} catch (Exception ex) {
						HbaseLog.l.warn("HWriter:mergeScalar > Ignore Unlock exp :" , ex);
					}
					continue;
				}
				
				Put update = ( null == lock ) ? new Put(pk) :  new Put(pk, lock);
				update.add(famB,nameB, data);
				update.setWriteToWAL(true);
				updates.add(update);
			}
			
			table.put(updates);
			table.flushCommits();

		} finally {
			boolean goodTable = true;
			for (RowLock lock: locks) {
				try { 
					if ( null != lock ) table.unlockRow(lock); 
				} catch (Exception ex) {
					reportUnlockException(ex);
					goodTable = false;
				}
			}
			
			if ( null != facade && null != table && goodTable) {
				facade.putTable(table);
			}
			
			if ( null != locks ) ObjectFactory.getInstance().putRowLockList(locks);
			if ( null != updates ) ObjectFactory.getInstance().putPutsList(updates);
		}
	}

	/**
	 * Merge a record accessing the existing value
	 * It happens with the locking mechanism
	 * @param tableName		Table name
	 * @param record	A record
	 * @throws IOException
	 */
	public void merge(String tableName, Record record) 
	throws IOException {
			
		if ( null == tableName  || null == record) return;
		if  (DEBUG_ENABLED) 
			HbaseLog.l.debug("HWriter:merge Record (" + tableName + ")") ;

   		HTableWrapper table = null;
		HBaseFacade facade = null;
		RowLock lock = null;
		
		try {
			byte[] pk = record.pk.toBytes();

			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);

			//Step 0 : If does exists no need to merge.. Just insert.
			Get existsCheck = new Get(pk);
			if ( ! table.exists(existsCheck) ) {
				insert(tableName, record);
				return;
			}
			
			//Step 1 : Aquire a lock before merging
			if  (DEBUG_ENABLED)  HbaseLog.l.debug("HWriter> Locking Row " );
			lock = table.lockRow(pk);
			if ( null == lock) {
				throw new IOException("HWriter:merge  Unable to aquire lock," + new String(pk) + 
					" for the table - " + tableName);
			}
			Get existingGet = ( null == lock) ? new Get(pk) : new Get(pk, lock);
			for (NV nv : record.getBlankNVs()) {
				existingGet = existingGet.addColumn(nv.family.toBytes(), nv.name.toBytes());
			}
			
			//Step 2 : Merge data with existing values
			Result r = table.get(existingGet);
			if ( null != r) {
				if ( null != r.list()) {
					for (KeyValue kv : r.list()) {
						byte[] existingB = kv.getValue();
						if ( null == existingB) continue;
						if ( 0 == existingB.length)continue;
						record.merge(kv.getFamily(),kv.getQualifier(), existingB);
					}
				}
			}
			
			//Step 3 : Only add values which have changed. 
			Put update = ( null == lock ) ? new Put(pk) :  new Put(pk, lock);
			int totalCols = 0;
			for (NV nv : record.getNVs()) {
				byte[] data = nv.data.toBytes();
				if ( nv.isDataUnchanged) continue;
				if  (DEBUG_ENABLED)  HbaseLog.l.debug("HWriter> data Size " + data.length);
				update = update.add(nv.family.toBytes(), nv.name.toBytes(), data);
				totalCols++;
			}
			
			//Step 4 : If no change.. Nothing to do. 
			if ( totalCols == 0 ) return;
			
			
			//Step 5 : Write the changes. 
			update.setWriteToWAL(true);
			if  (DEBUG_ENABLED)  HbaseLog.l.debug("HWriter> Committing Updates" );
			table.put(update);
			table.flushCommits();

		} finally {
			
			boolean goodTable = true;
			if ( null != lock ) {
				if  (DEBUG_ENABLED)  HbaseLog.l.debug("HWriter> Un Locking Row " );
				try { table.unlockRow(lock); } catch (Exception ex) {
					reportUnlockException(ex);
					goodTable = false;
				}
			}
			if ( null != facade && null != table && goodTable) {
				facade.putTable(table);
			}
		}
	}
	
	private void reportUnlockException(Exception ex) {
		Runtime runTime = Runtime.getRuntime();
		String errorMsg = "Max Mem: " + runTime.maxMemory()/1024; 
		errorMsg = errorMsg + ", Total Mem: " + runTime.totalMemory()/1024; 
		errorMsg = errorMsg + ", Free Mem: " + runTime.freeMemory()/1024; 
		HbaseLog.l.warn("HWriter:reportUnlockException > Ignoring Unlock exp. May be memory Issue \n" + errorMsg, ex);
	}
	
}