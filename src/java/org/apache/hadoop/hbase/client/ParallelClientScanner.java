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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * An implementation of the {@link ResultScanner} that co-ordinates with the
 * {@link ParallelScannerManager}.
 */
public class ParallelClientScanner implements ResultScanner {
  private boolean isClosed = false;
  private ParallelScannerManager parallelScannerManager;
  private LinkedList<Result> preFetchedResults = new LinkedList<Result>();

  /**
   * Constructor.
   * 
   * @param table the table name
   * @param scan the scan
   * @param preFetchCount the number of rows to fetch at once
   * 
   * @throws IOException
   *           if an error occurs
   */
  public ParallelClientScanner(ParallelHTable table, Scan scan,
    int preFetchCount)
  throws IOException {
    // set of the parallel scanner (which will start connecting to regions and
    // fetching data)
    parallelScannerManager = new ParallelScannerManager(table, scan,
        preFetchCount);
  }

  @Override
  public Result next() throws IOException {
    // it's possible that the underlying resources have been closed in a
    // previous call to this method
    // in that case just return null
    if (isClosed) {
      return null;
    }

    // if there are no pre-fetched results then try and get more
    if (preFetchedResults.isEmpty()) {
      Result[] results = parallelScannerManager.next();
      if (results != null) {
        preFetchedResults.addAll(Arrays.asList(results));
      }
    }

    // if there are still no pre-fetched results then we've exhausted all
    // regions
    // close all the regions and return null
    if (preFetchedResults.isEmpty()) {
      close();
      return null;
    }

    return preFetchedResults.poll();
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    List<Result> results = new ArrayList<Result>();
    while (results.size() < nbRows) {
      Result result = next();
      if (result != null) {
        results.add(result);
      } else {
        // all regions are exhausted, nothing more to do...
        break;
      }
    }

    return results.toArray(new Result[results.size()]);
  }

  @Override
  public void close() {
    if (!isClosed) {
      isClosed = true;
      parallelScannerManager.close();
    }
  }

  @Override
  public Iterator<Result> iterator() {
    return new ResultIterator(this);
  }

  private static class ResultIterator implements Iterator<Result> {
    // The next RowResult, possibly pre-read
    private Result next = null;
    private ResultScanner resultScanner = null;

    ResultIterator(ResultScanner resultScanner) {
      this.resultScanner = resultScanner;
    }

    // return true if there is another item pending, false if there isn't.
    // this method is where the actual advancing takes place, but you need
    // to call next() to consume it. hasNext() will only advance if there
    // isn't a pending next().
    public boolean hasNext() {
      if (next == null) {
        try {
          next = resultScanner.next();
          return next != null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return true;
    }

    // get the pending next item and advance the iterator. returns null if
    // there is no next item.
    public Result next() {
      // since hasNext() does the real advancing, we call this to determine
      // if there is a next before proceeding.
      if (!hasNext()) {
        return null;
      }

      // if we get to here, then hasNext() has given us an item to return.
      // we want to return the item and then null out the next pointer, so
      // we use a temporary variable.
      Result temp = next;
      next = null;
      return temp;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
