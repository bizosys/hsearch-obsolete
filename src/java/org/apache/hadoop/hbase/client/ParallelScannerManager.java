/*
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

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Manages scanning in parallel.
 */
public class ParallelScannerManager {
  static final Logger LOG = Logger.getLogger(ParallelScannerManager.class);
  private HTable table;
  private volatile int preFetchCount;
  private ExecutorCompletionService<RegionCallableResult> completionService;
  private int pendingTasks = 0;
  private List<RegionCallable> callables;
  private boolean isClosed = false;

  /**
   * Constructor.
   * @param table         the name
   * @param scan          the scan
   * @param preFetchCount the pre fetch count
   * @throws IOException if an error occurs
   */
  public ParallelScannerManager(ParallelHTable table, Scan scan,
                                int preFetchCount)
      throws IOException {
    this.table = table;
    this.preFetchCount = preFetchCount;
    callables = new ArrayList<RegionCallable>();
    ExecutorService threadPool = table.getExecutorService();
    completionService =
        new ExecutorCompletionService<RegionCallableResult>(threadPool);
    Set<HRegionInfo> regions = table.getRegionsInfo().keySet();
    for (HRegionInfo region : regions) {
      /*
        The logic below determines if the region should be included in the scan.
        It handles the case when the scan has specified a startRow and/or stopRow.
       */
      boolean isScanInterestedInRegion = (scan.getStartRow().length == 0
          && scan.getStopRow().length == 0) || regions.size() == 1;

      if (!isScanInterestedInRegion) {
        byte[] regionStartRow = region.getStartKey();
        byte[] regionEndRow = region.getEndKey();

        isScanInterestedInRegion = isScanInterestedInRegion(scan,
            regionStartRow, regionEndRow);
      }

      if (isScanInterestedInRegion) {
        submitTask(scan, region);
      }
    }
  }

  protected static boolean isScanInterestedInRegion(Scan scan,
                                                    byte[] regionStartRow,
                                                    byte[] regionEndRow) {
    byte[] scanStartRow = scan.getStartRow();
    byte[] scanStopRow = scan.getStopRow();

    boolean scanStartRowLessOrEqualsRegionEndRow = isEmpty(scanStartRow)
        || isEmpty(regionEndRow)
        || isLesserEqual(scanStartRow, regionEndRow);
    boolean scanStopRowGreaterOrEqualsRegionStartRow = isEmpty(scanStopRow)
        || isEmpty(regionStartRow)
        || isGreaterEqual(scanStopRow, regionStartRow);

    return scanStartRowLessOrEqualsRegionEndRow
        && scanStopRowGreaterOrEqualsRegionStartRow;
  }

  private static boolean isEmpty(byte[] bytes) {
    return bytes == null || bytes.length == 0;
  }

  private static boolean isLesserEqual(byte[] left, byte[] right) {
    return Bytes.compareTo(left, right) <= 0;
  }

  private static boolean isGreaterEqual(byte[] left, byte[] right) {
    return Bytes.compareTo(left, right) >= 0;
  }

  private void submitTask(Scan scan, HRegionInfo regionInfo) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Submitting fetch task for region %s",
          regionInfo));
    }
    RegionCallable regionCallable = new RegionCallable(table, scan, regionInfo,
        preFetchCount);
    callables.add(regionCallable);
    reSubmitTask(regionCallable);
  }

  /**
   * Submits the callable as a task and increments the counter to keep track of
   * the number of pending tasks.
   * @param callable the callable task
   */
  private void reSubmitTask(final RegionCallable callable) {
    if (isClosed) {
      return;
    }
    completionService.submit(new Callable<RegionCallableResult>() {
      @Override
      public RegionCallableResult call() throws Exception {
        return table.getConnection().getRegionServerWithRetries(callable);
      }
    });
    pendingTasks++;
  }

  /**
   * Has the same semantics as {@link java.util.concurrent.CompletionService#take()}
   * except that the counter of pending tasks is decremented when a task is
   * taken from the queue. Also, this method will immediately return null if
   * there are no more pending tasks queued.
   * @return the callable result or null if no more tasks are pendingt
   * @throws java.util.concurrent.ExecutionException
   *          if the execution of the task failed
   */
  private RegionCallableResult takeTask() throws ExecutionException {
    // if there are no more pending tasks then we have run out of things to
    // do...
    if (pendingTasks == 0) {
      return null;
    }

    try {
      Future<RegionCallableResult> future = completionService.take();
      pendingTasks--;
      return future.get();
    } catch (InterruptedException e) {
      // we were intertupted, recurse and wait to take another
      return takeTask();
    }
  }

  /**
   * Used to determine if all the managed scanner callables are exhausted.
   */
  private boolean isCompletionServiceEmpty() {
    return pendingTasks == 0;
  }

  /**
   * Returns the next array of results.
   * @return the next array of results or null if there are no more results
   * @throws IOException if an error occurs
   */
  public Result[] next() throws IOException {
    // wait for a result to come back
    RegionCallableResult result = null;
    try {
      result = takeTask();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RegionSplitException) {
        throw ((RegionSplitException) e.getCause()).getCause();
      } else if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IllegalStateException(e);
      }
    }

    if (result == null) {
      // all regions are out of records, that's it we're finished
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("All regions exhausted..."));
      }
      return null;
    } else if (result.getResults() == null) {
      // all regions are out of records, that's it we're finished
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("Region exhausted, Region: %s, Count: %s",
            result.getRegionCallable().getRegionInfo(), result
                .getRegionCallable().getResultsConsumed()));
      }
      // that region is been exhausted, recurse to get another regions results
      return next();
    } else {
      // there are results, so take them and resubmit that region to get more
      // results
      Result[] results = result.getResults();
      // resubmit the task to get the next batch
      reSubmitTask(result.getRegionCallable());
      return results;
    }
  }

  /**
   * Clear any running tasks and close the region callables.
   */
  public void close() {
    isClosed = true;
    // wait until all the workers have completed
    while (!isCompletionServiceEmpty()) {
      try {
        takeTask();
      } catch (ExecutionException e) {
        // do nothing
        LOG.debug("Ignore", e);
      }
    }

    // close the individual region callables
    for (RegionCallable callable : callables) {
      try {
        callable.close();
      } catch (Exception e) {
        // just keep going
        LOG.debug("Ignore", e);
      }
    }
  }

  /**
   * The region callable.
   */
  protected static class RegionCallable extends
      ServerCallable<RegionCallableResult> {
    static final Logger LOG = Logger.getLogger(RegionCallable.class);

    private boolean instantiated = false;
    private long scannerId = -1L;
    private Scan scan;
    private int preFetchCount;

    private byte[] lastConsumedKey;
    private long resultsConsumed;

    public RegionCallable(HTable table, Scan scan, HRegionInfo regionInfo, int preFetchCount) {
      super(table.getConnection(), table.getTableName(), regionInfo.getStartKey());
      this.scan = scan;
      this.preFetchCount = preFetchCount;
    }

    @Override
    public void instantiateServer(boolean reload) throws IOException {
      if (!instantiated || reload) {
        super.instantiateServer(reload);
        instantiated = true;
      }
    }

    public RegionCallableResult call() throws IOException {
      if (server == null) {
        throw new IllegalStateException("The server hasn't been instantiated " +
            "yet.  Ensure that you've called the " + "instantiateServer method " +
            "or that you are using the connection.getRegionServerWithRetries " +
            "method");
      }

      if (scannerId == -1) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Opening scanner on region: %s",
              super.location.getRegionInfo()));
        }
        this.scannerId = server.openScanner(this.location.getRegionInfo()
            .getRegionName(), scan);
      }

      Result[] results = null;
      try {
        /* if (LOG.isDebugEnabled()) {LOG.debug(String.format(
         * "Fetching results using scannerId: %s from region: %s", scannerId,
         * regionInfo)); }
         */
        results = server.next(scannerId, preFetchCount);
      } catch (IOException e) {
        IOException ioe = null;
        if (e instanceof RemoteException) {
          ioe =
              RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (ioe != null && ioe instanceof NotServingRegionException) {
          // Throw a DNRE so that we break out of cycle of calling NSRE
          // when what we need is to open scanner against new location.
          // Attach NSRE to signal client that it needs to resetup scanner.
          throw new RegionSplitException(this, (NotServingRegionException) ioe);
        }
      }

      boolean resultEmpty = isResultEmpty(results);
      if (!resultEmpty) {
        this.lastConsumedKey = results[results.length - 1].getRow();
        this.resultsConsumed += results.length;
      }
      return new RegionCallableResult(this, resultEmpty ? null : results);
    }

    private boolean isResultEmpty(Result[] results) {
      return results == null || results.length == 0;
    }

    /**
     * Close the callable which in turn closes the scanner.
     */
    public void close() {
      if (this.scannerId == -1L) {
        return;
      }
      try {
        this.server.close(this.scannerId);
      } catch (IOException e) {
        LOG.warn("Ignore, probably already closed", e);
      }
      this.scannerId = -1L;
    }

    public byte[] getLastConsumedKey() {
      return lastConsumedKey;
    }

    public long getResultsConsumed() {
      return resultsConsumed;
    }

    public HRegionInfo getRegionInfo() {
      return super.location.getRegionInfo();
    }

    public Scan getScan() {
      return scan;
    }
  }

  /**
   * The wrapper result returned from a region callable.
   */
  protected static class RegionCallableResult {
    private RegionCallable regionCallable;
    private Result[] results;

    /**
     * Constructor.
     * @param regionCallable the callable
     * @param results        the result
     */
    public RegionCallableResult(RegionCallable regionCallable, Result[] results) {
      this.regionCallable = regionCallable;
      this.results = results;
    }

    public RegionCallable getRegionCallable() {
      return regionCallable;
    }

    public Result[] getResults() {
      return results;
    }
  }

  /**
   * A specialised exception to indicate that the region has split and that it
   * should not be retried.
   */
  @SuppressWarnings("serial")
  protected static class RegionSplitException extends DoNotRetryIOException {
    private transient RegionCallable regionCallable;
    private transient NotServingRegionException cause;

    /**
     * Constructor.
     * @param regionCallable the callable
     * @param cause          the underlying cause
     */
    public RegionSplitException(RegionCallable regionCallable,
                                NotServingRegionException cause) {
      super();
      this.regionCallable = regionCallable;
      this.cause = cause;
    }

    public RegionCallable getRegionCallable() {
      return regionCallable;
    }

    public NotServingRegionException getCause() {
      return cause;
    }
  }
}
