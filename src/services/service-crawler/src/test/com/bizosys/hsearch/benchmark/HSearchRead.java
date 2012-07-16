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
package com.bizosys.hsearch.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.index.IndexReader;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class HSearchRead {

	String fileName = null;
	String docsInAPage = "10";

	int threadsT = 0;

	int shots = 0;

	AccountInfo acc = null;
	String ANONYMOUS = "anonymous";
	
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);
		
		this.acc = Account.getAccount(ANONYMOUS);
		if ( null == acc) {
			acc = new AccountInfo(ANONYMOUS);
			acc.name = ANONYMOUS;
			acc.maxbuckets = 1;
			Account.storeAccount(acc);
		}
		
	}

	protected void tearDown() {
		ServiceFactory.getInstance().stop();
	}

	public static void main(String[] args) throws Exception {
		
		HSearchRead test = new HSearchRead();
		test.checkCommandLine(args);
		test.setUp();

		Parallalization[] pls = test.parallelize();

		StringBuilder sb = new StringBuilder();
		int threadsT = pls.length;
		Set<Integer> doneList = new HashSet<Integer>();
		for (int i = 0; i < threadsT; i++) {
			if (pls[i].status == 0 || pls[i].status == 1) {
				if (doneList.contains(i)) continue;
				doneList.add(i);
				sb.append(pls[i]);
			} else {
				if ( Thread.State.TERMINATED.equals(pls[i].thread.getState() ) ) continue;
				System.out.println("Waitting for the query:" + pls[i].keyword);
				pls[i].thread.join(180000);
				doneList.add(i);
				sb.append(pls[i]);
			}
		}

		long max = 0, min = Long.MAX_VALUE, totalDuration = 0;
		for (int i = 0; i < threadsT; i++) {
			totalDuration = totalDuration + pls[i].duration;
			if (max < pls[i].duration) max = pls[i].duration;
			if (min > pls[i].duration) min = pls[i].duration;
		}
		sb.append("\nThreads:").append(threadsT).append(" , Avg: ").append(
				totalDuration / threadsT).append("(ms) Max: ").append(max)
				.append("(ms), Min: ").append(min).append("(ms)");

		System.out.println(sb.toString());
		for (Parallalization pl : pls) pl.printResult();

		test.tearDown();

	}

	public void checkCommandLine(String[] args) throws Exception {

		String[] flds = new String[] { "filename", "docsInAPage", "threads", "shots" };
		int fldIndex = 0;
		
		if (args.length != 4) {
			String msg = "Usage >> \n<<filename>> \n" + "<<docsInAPage>> 10\n" + "<<threads>> 1\n"
					+ "<<shots>> 5";
			throw new Exception(msg);
		}
		
		for (String arg : args) {
			System.out.println(flds[fldIndex++] + "=" + arg);
		}
		
		fldIndex = 0;
		this.fileName = args[fldIndex++];
		this.docsInAPage = args[fldIndex++];
		this.threadsT = new Integer(args[fldIndex++]);
		this.shots = new Integer(args[fldIndex++]);

	}

	private Parallalization[] parallelize() throws Exception {

		if (threadsT == 0) threadsT = 1;
		Parallalization[] pls = new Parallalization[threadsT];
		for (int i = 0; i < threadsT; i++) {
			int st = 1 + shots * i; //1
			int ed = st + shots; //2
			System.out.println("Setting a thread with Range starts from " + st + " till " + ed);
			Parallalization pl = new Parallalization(this.acc, fileName, docsInAPage, st, ed);
			pl.thread = new Thread(pl);
			pl.thread.start();
			pls[i] = pl;
		}
		return pls;
	}

	public static class Parallalization implements Runnable {

		public long duration = 0;

		private int status = -1;

		int st = 0;

		int ed = 0;

		QueryResult res = null;
		
		QueryContext ctx =  null;

		String fileName;

		String threadName = "Client ";

		String keyword = null;

		Thread thread = null;
		
		String docsInAPage = "10";

		AccountInfo acc = null;
		
		public Parallalization(AccountInfo acc, String fileName, String docsInAPage, int st, int ed) {
			this.acc = acc;
			this.fileName = fileName;
			this.st = st;
			this.ed = ed;
			this.docsInAPage = docsInAPage;
		}

		private QueryResult search() throws Exception {
			String query = this.keyword + " dfl:" + docsInAPage;
			System.out.println(threadName + " Search = [" + query + "]");
			status = 106;
			
			this.ctx = new QueryContext(this.acc, query);
			this.res = IndexReader.getInstance().search(ctx);
			status = 108;
			return res;
		}

		public void run() {
			status = 101;
			threadName = threadName + Thread.currentThread().getName();
			status = 102;
			try {

				loadKeyword();
				status = 103;
				long startTime = System.currentTimeMillis();
				this.res = search();
				long endTime = System.currentTimeMillis();
				status = 1;
				this.duration = endTime - startTime;

			} catch (Exception ex) {
				status = 0;
				System.err.println(keyword + " Search Failure" + ex.getMessage());
				ex.printStackTrace(System.err);
			} finally {
				System.out.println(keyword + "..search over..");
			}
		}

		public synchronized void loadKeyword() throws Exception {

			BufferedReader reader = null;
			InputStream stream = null;

			try {
				File aFile = FileReaderUtil.getFile(fileName);
				if ( aFile.exists()) {
					stream = new FileInputStream(aFile);
					reader = new BufferedReader(new InputStreamReader(stream));
	
					int lineNo = 1;
					while ((keyword = reader.readLine()) != null) {
						if ( (lineNo + 1) > ed) break;
						if (lineNo == st) break;
						if (lineNo++ < st) continue;
					}
				}
			} catch (Exception ex) {
				status = 0;
				System.err.println("Search Keyword Reader Failure" + ex.getMessage());
				ex.printStackTrace(System.err);
			} finally {
				try {
					if (null != reader) reader.close();
				} catch (Exception ex) {
					System.err.println(threadName + " util.FileReaderUtil" + ex.getMessage());
					ex.printStackTrace(System.err);
				}
				try {
					if (null != stream) stream.close();
				} catch (Exception ex) {
					System.err.println(threadName + " util.FileReaderUtil" + ex.getMessage());
					ex.printStackTrace(System.err);
				}
				System.out.println("..One parallel thread over..");
			}
		}

		public String getStatus() {
			String strStatus = "Not Started";
			switch (status) {
			case -1:
				strStatus = "Not Started";
				break;
			case 0:
				strStatus = "Failed";
				break;
			case 1:
				strStatus = "Sucessful";
				break;
			default:
				strStatus = new Integer(status).toString();
			}
			return strStatus;
		}

		public String toString() {
			int records = (null == res) ? 0 : 
				(res.teasers == null) ? 0 : res.teasers.length;
			int statics = (null == res) ? 0 : 
				(res.sortedStaticWeights == null) ? 0 : res.sortedStaticWeights.length;
			int dynamic = (null == res) ? 0 : 
				(res.sortedDynamicWeights == null) ? 0 : res.sortedDynamicWeights.length;

			return "\n#### > " + keyword + ", Run Status = " + getStatus()
					+ " , Static = " + statics + " , Dynamic = " + dynamic 
					+ " , Teasers = " + records + " , Time taken = " + duration;
		}

		public void printResult() {
			if (null == res) return;
			System.out.println("Static =>" + res.sortedStaticWeights.length);
			if ( null != res.sortedDynamicWeights) System.out.println(
				"Dynamic =>" + res.sortedDynamicWeights.length);
			
			if (null == res.teasers) return;
			int i = 0;
			for (Object teaserO : res.teasers) {
				if (null == teaserO) continue;
				DocTeaser teaser = (DocTeaser) teaserO; 
				System.out.println(keyword + "..." + i++ + "  >>" + teaser.toString());
			}
		}
	}
	
	public void printIndex(){
		try {
			long bucketId = -9223372036854775808L;
			System.out.println( IndexReader.getInvertedIndex(bucketId) ) ;
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}

}
