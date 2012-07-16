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


import java.io.File;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

public class LuceneWikipediaW {
	String fileName = null;
	String detailUrlPrefix = null;
	String idFldName = null;
	String[] titleFldNames = null;
	String[] previewFlds = null;
	String dictIndexableFld = null;
	String dictRecordType = null;
	int startIndex = 0;
	int batchSize = 300;
	String runPlan = null;
	int endIndex = 0;
	int threads = 0;
	HDocument doc = null;

	public static void main(String[] args)throws Exception {

		LuceneIndexManager.getInstance();
		
		LuceneWikipediaW fbb = new LuceneWikipediaW();
		fbb.checkCommandLine(args);
		System.out.println("Starting the services");


		long start = System.currentTimeMillis();
		if ( fbb.threads > 1) {
			if ( fbb.endIndex == -1) 
				fbb.endIndex = FileReaderUtil.getLineCount(fbb.fileName);
			int totalRecs = fbb.endIndex - fbb.startIndex;
			totalRecs = totalRecs / fbb.threads;
			
			Parallalization[] p13nL = new Parallalization[fbb.threads];
			for ( int i=0; i<fbb.threads; i++ ) {
				int st = fbb.startIndex + totalRecs * i ;
				int ed = st + totalRecs;
				System.out.println("Setting a thread From-To = " + st + "-" +  ed);
				LuceneXmlFileCrawler  tff = fbb.create(args,st, ed);
				Parallalization p13n = new Parallalization(tff);
				p13nL[i] = p13n ;
				Thread x = new Thread(p13n);
				x.start();
				try { Thread.sleep(10);}catch (Exception ex){}
			}
			
			int totalDone = 0;
			int totalSucess = 0;
			int totalFailed = 0;
			for ( int i=0; i<fbb.threads; i++ ) {
				
				if ( p13nL[i].status == -1 ) {
					Thread.sleep(2000);
					i--;
					continue;
				}
				
				totalDone++;
				if ( p13nL[i].status == 1) totalSucess++;
				else totalFailed++;
				
				if ( totalDone == fbb.threads){
					long end = System.currentTimeMillis();
					int records = (fbb.endIndex - fbb.startIndex);
					long duration = (end - start);
					StringBuilder sb = new StringBuilder();
					sb.append(" ####################  Test Result Reporting  #################### ");
					sb.append("\nProcessed: " + totalDone + " , Sucess: " + totalSucess + " , Failed: " + totalFailed);
					long max = 0, min = Long.MAX_VALUE; 
					for ( i=0; i< fbb.threads; i++) {
						if ( max < p13nL[i].duration ) max = p13nL[i].duration;
						if ( min > p13nL[i].duration ) min = p13nL[i].duration;
					}
					sb.append("\nThreads:").append(fbb.threads).append(
							" , Avg: ").append(duration/records).append( 
							"(ms) Max: ").append(max/records).append(
							"(ms), Min: ").append(min/records).append("(ms)");
					System.out.println(sb.toString());
					break;
				}
			}
		} else {
			LuceneXmlFileCrawler tff = fbb.create(args, fbb.startIndex,fbb.endIndex);
			tff.fetchAndIndex();
			LuceneIndexManager.getInstance().close();
			long end = System.currentTimeMillis();
			System.out.println("Threads:" + fbb.threads + " , Time taken(ms):" + (end - start) );
		}
	}
	
	public LuceneXmlFileCrawler create(String[] args, int fromIndex, int toIndex) throws Exception {
		File dir = new File(fileName);
		LuceneXmlFileCrawler tw = new LuceneXmlFileCrawler(dir.getAbsolutePath(),
			doc, idFldName, titleFldNames, previewFlds, fromIndex, toIndex, batchSize); 
		return tw;
	}
	
	/**
	 * Parallelizing the task.
	 * @author karan
	 *
	 */
	public static class Parallalization implements Runnable {
		
		LuceneXmlFileCrawler bt = null;
		public int status = -1;
		public long duration = 0;
		
		public Parallalization(LuceneXmlFileCrawler bt) {
			this.bt = bt;
		}
		public void run() {
			long start = System.currentTimeMillis();
			try {
				this.bt.fetchAndIndex();
				status = 1;
			} catch (Exception ex) {
				status = 0;
				System.err.println(ex);
			} finally {
				duration = System.currentTimeMillis() - start;
			}
		}
	}	
	
	/**
	 * Check the inout commandline before processing. 
	 * We expect proper parameters. 
	 * @param args
	 * @throws Exception
	 */
	public void checkCommandLine(String[] args) throws SystemFault {

		if ( args.length != 9) {
			String msg = 
				"Usage >> \n<<filename>> \n" +
				"<<idFldName>> id\n" +
				"<<doctype>> docType\n" +
				"<<titleFldNames>> name\n" +
				"<<previewFlds>> name,geolocation,containedby\n" +
				"<<startIndex>> 0 Default \n" +
				"<<endIndex>> -1 for no limit\n" +
				"<<batchsize>> 300\n" + 
				"<<totalthreads>>";
			System.err.println(msg);
		}
		
		String[] flds = new String[]{"filename", "docType", "idFldName",
				"titleFldNames","previewFlds","startIndex",
				"endIndex","batchsize", "totalthreads"};
		int fldIndex=0;
		for(String arg : args) {
			System.out.println(flds[fldIndex++] + "=" + arg);
		}
		
		fldIndex = 0;
		this.fileName = args[fldIndex++];
		String recordTag = args[fldIndex++];
		this.idFldName = args[fldIndex++];
		this.titleFldNames = StringUtils.getStrings(args[fldIndex++], ",");
		this.previewFlds = StringUtils.getStrings(args[fldIndex++], ",");
		this.startIndex = new Integer(args[fldIndex++]);
		this.endIndex = new Integer(args[fldIndex++]);
		this.batchSize = new Integer(args[fldIndex++]);
		this.threads = new Integer(args[fldIndex++]);
		
		this.doc = new HDocument();
		this.doc.docType = recordTag;
	}
}
