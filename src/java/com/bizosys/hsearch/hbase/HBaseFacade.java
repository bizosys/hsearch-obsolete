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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Initializes and Serves HBase resources from here
 * @author Abinasha Karana
 *
 */
public class HBaseFacade {

	/**
	 * Configuration Settings
	 */
	protected Configuration conf;
	
	/**
	 * HBase admin
	 */
	protected HBaseAdmin admin = null;
	
	/**
	 * HBase table description
	 */
	protected HTableDescriptor desc = null;
	
	/**
	 * Singleton instance
	 */
	private static HBaseFacade instance = null;
	
	/**
	 * Give a static singleton instance
	 * @return	HBaseFacade
	 * @throws IOException
	 */
	public static HBaseFacade getInstance() throws IOException
	{
		if ( null != instance ) return instance;
		synchronized (HBaseFacade.class) {
			if ( null != instance ) return instance;
			instance = new HBaseFacade();
		}
		return instance;
	}

	/**
	 * Initialized HBase administrator reading the configuration file
	 * @throws IOException
	 */
	private HBaseFacade() throws IOException{
		HbaseLog.l.debug("HBaseFacade > Initializing HBaseFacade");
		conf = HBaseConfiguration.create();
		try {
			admin = new HBaseAdmin(conf);
			HBaseFacade.instance = this;
			HbaseLog.l.debug("HBaseFacade > HBaseFacade initialized.");
		} catch (MasterNotRunningException ex) {
			throw new IOException ("HBaseFacade > HBase Master instance is not running..");			
		}
	}
	
	/**
	 * Don't use the admin.shutdown() - This shuts down the hbase instance.
	 *
	 */
	public void stop() {
		try {
			admin.shutdown();
		} catch (IOException ex) {
			HbaseLog.l.warn("HBAseFacade:stop()", ex);
		}
	}
	
	/**
	 * Get the HBase admin
	 * @return	HBase Admin Object
	 * @throws IOException
	 */
	public HBaseAdmin getAdmin() throws IOException {
		if ( null == admin) throw new IOException ("HBaseFacade > HBase Service is not initialized");
		return admin;
	}
	
	public Configuration getHBaseConfig() {
		return this.conf;
	}
	
    /**
     * HBase table pool
     */
	HTablePool pool = null;
	
	/**
	 * Number of current live tables
	 */
    int liveTables = 0; 
    
    /**
     * Get a Wrapped HBase table
     * @param tableName	The table name
     * @return	Wrapped HBase table
     * @throws IOException
     */
	public HTableWrapper getTable(String tableName) throws IOException {
		
		if ( null == pool ) pool = new HTablePool(this.conf, Integer.MAX_VALUE);
		//if ( HLog.l.isDebugEnabled() ) HLog.l.debug("HBaseFacade > Live hbase tables : " + liveTables); 
		
		HTableWrapper table = new HTableWrapper( tableName, pool.getTable(tableName));
		liveTables++;
		return table;
	}

	/**
	 * Returns the wrapped table to the pool for recycling
	 * @param table	The Wrapped HBase table
	 */
	public void putTable(HTableWrapper table) {
		if ( null == pool ) return;
		pool.putTable(table.table);
		liveTables--;
	}
	
	/**
	 * Recycles a table
	 * @param table
	 * @throws IOException
	 */
	public void recycleTable(HTableWrapper table) throws IOException {
		if ( null == pool ) return;
		pool.putTable(table.table);
		table.table = pool.getTable(table.tableName);
	}	
	
	/**
	 * Monitoring purposes keeps counting active table connections in operation 
	 * @return	Number of connections made
	 */
	public int getLiveTables() {
		return this.liveTables;
	}
}