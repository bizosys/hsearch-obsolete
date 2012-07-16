/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * {@inheritDoc}
 * <p/>
 * This extension of the {@link HTable} class provides a mechanism to initiate
 * and run scanners for each region in parallel. This is achieved using the a
 * {@link ExecutorService} and a {@link ServerCallable} for each region. Each
 * callable in responsible for fetching 'hbase.client.scanner.caching'* rows
 * from its region per invocation.
 * <p/>
 * In order to limit the number of rows pulled into memory on the client the
 * region callable is not resubmitted to the {@link ExecutorService} until it's
 * previous set of results has been consumed. As a result the order of the rows
 * will not necessarily be in key order. This has several side effects;
 * <ul>
 * <li>The provided {@link Scan} cannot specify a
 * {@link Scan#setStartRow(byte[]) start row} or a
 * {@link Scan#setStopRow(byte[]) stop row}.
 * <li>The provided {@link Scan#getFilter()} cannot abort the result set
 * processing using the {@link Filter#filterAllRemaining()} method.
 * </ul>
 * <p/>
 ** If the 'hbase.client.scanner.caching' results in 1 then the value is over
 * written with {@link #DEFAULT_SCANNER_CACHING} value.
 */
public class ParallelHTable extends HTable {
  /**
   * Default scanner caching value.
   */
  public static final int DEFAULT_SCANNER_CACHING = 2000;

  private ExecutorService executorService;

  /**
   * Constructor.
   * 
   * @param tableName the table name
   * @param executorService the executor service
   * 
   * @throws IOException
   *           if an error occurs
   */
  public ParallelHTable(String tableName, ExecutorService executorService)
      throws IOException {
    super(tableName);
    this.executorService = executorService;
  }

  /**
   * Constructor.
   * 
   * @param tableName the table name
   * @param executorService the executor service
   * 
   * @throws IOException if an error occurs
   */
  public ParallelHTable(byte[] tableName, ExecutorService executorService)
      throws IOException {
    super(tableName);
    this.executorService = executorService;
  }

  /**
   * Constructor.
   * 
   * @param conf the config
   * @param tableName the table name
   * @param executorService the executor service
   * 
   * @throws IOException if an error occurs
   */
  public ParallelHTable(HBaseConfiguration conf, String tableName,
      ExecutorService executorService) throws IOException {
    super(conf, tableName);
    this.executorService = executorService;
  }

  /**
   * Constructor.
   * 
   * @param conf the config
   * @param tableName the table name
   * @param executorService the executor service
   * 
   * @throws IOException if an error occurs
   */
  public ParallelHTable(Configuration conf, byte[] tableName,
      ExecutorService executorService) throws IOException {
    super(conf, tableName);
    this.executorService = executorService;
  }

  /**
   * Get a scanner on the current table as specified by the {@link Scan} object.
   * Also note that if the {@link ParallelClientScanner} is used then region
   * splits will NOT be handled. An NotServingRegionException will be thrown and
   * the query should be re-tried by the client.
   * 
   * @param scan a configured {@link Scan} object
   * @param scanInParallel if true multiple thread will be used to perform the scan
   * @return scanner
   * 
   * @throws IOException if an error occurs
   */
  public ResultScanner getScanner(final Scan scan, boolean scanInParallel) throws IOException {
    if (scanInParallel) {
      return new ParallelClientScanner(this, scan, defaultScannerCaching());
    } else {
      return super.getScanner(scan);
    }
  }

  /**
   * The default scanner caching (pre-fetch count in our code) is set to 1 in
   * HTable. That's not really suitable for the parallel scanner, so instead we
   * use {@link #DEFAULT_SCANNER_CACHING}.
   * 
   * @return the value of {@link HTable#scannerCaching} if it's not set to 1,
   *         otherwise {@link #DEFAULT_SCANNER_CACHING}
   */
  private int defaultScannerCaching() {
    return (super.scannerCaching != 1 ? super.scannerCaching
        : DEFAULT_SCANNER_CACHING);
  }

  /**
   * Returns the {@link java.util.concurrent.ExecutorService} used to process
   * the parallel region scans.
   * 
   * @return the executor service
   */
  public ExecutorService getExecutorService() {
    return executorService;
  }
}
