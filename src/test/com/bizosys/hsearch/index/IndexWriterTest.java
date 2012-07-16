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
package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.TestAll;
import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Field;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.dictionary.DictEntry;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.lang.Stemmer;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.FileReaderUtil;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class IndexWriterTest extends TestCase {

	public static void main(String[] args) throws Exception {
		
		IndexWriterTest t = new IndexWriterTest();
		String[] modes = new String[] { "all", "random", "method"};
		String mode = modes[2];
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			//t.testInsertBatch("id", "name", "location");
			t.tearDown();
		}
	}

	boolean isMultiClient = true;
	AccountInfo acc = null;
	boolean concurrency = true;
	String ANONYMOUS = "anonymous";
	
	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);

		this.ANONYMOUS = Thread.currentThread().getName();
		this.acc = Account.getAccount(ANONYMOUS);
		if ( null == acc) {
			acc = new AccountInfo(ANONYMOUS);
			acc.name = ANONYMOUS;
			acc.maxbuckets = 15;
			Account.storeAccount(acc);
			System.out.println("New account is created");
		} else {
			acc.maxbuckets = 15;
			Account.storeAccount(acc);
			System.out.println("Account already exist");
		}
		
		IndexWriter.getInstance().truncate(ANONYMOUS);
		System.out.println( Account.getAccount(ANONYMOUS).toXml() ) ;

	}

	@Override
	protected void tearDown() throws Exception {
		ServiceFactory.getInstance().stop();
	}
	
	private void printInvertedIndex() throws ApplicationFault, SystemFault {
		List<InvertedIndex> ii = IndexReader.getInvertedIndex(-9223372036854775807L);
		if ( null == ii) return;
		for (InvertedIndex index : ii) {
			System.out.println(index.toString());
		}
		System.out.println("Inverted Index is printed");
	}

	public void testTenantBucketScoping() throws Exception {
		
		System.out.println("********** Set First Tenant ENTER **********\n\n\n");
		String names[]= new String[]{ this.ANONYMOUS + "1", this.ANONYMOUS + "2", this.ANONYMOUS + "3"};
		AccountInfo[] accInfos = new AccountInfo[3];
		long buckets[]= new long[]{ 0,0,0};

		
		for (int i=0; i< 3 ; i++) {
			System.out.println("********** Set Tenant[" + i + "] ENTER **********");

			accInfos[i] = Account.getAccount(names[i]);
			if ( null == accInfos[i]) {
				accInfos[i] = new AccountInfo(names[i]);
				accInfos[i].name = names[i];
				accInfos[i].maxbuckets = 1;
				Account.storeAccount(accInfos[i]);
				buckets[i] = Account.getCurrentBucket(accInfos[i]);
			} else {
				buckets[i] = Account.getCurrentBucket(accInfos[i]);
			}
			System.out.println("********** Set Tenant[" + i + "] EXIT **********");
		}
		
		for (int i=0; i< 2 ; i++) {
			System.out.println("********** Index Document,  Tenant[" + i + "] ENTER **********");
			
			HDocument hdoc = new HDocument();
			hdoc.key = "Key002";
			hdoc.tenant = accInfos[i].name;
			hdoc.title = "Abinash Karan";
			hdoc.fields = new ArrayList<Field>();
			hdoc.fields.add(new HField("BODY", "Bangalore"));
			hdoc.loadBucketAndSerials(accInfos[i]);
			
			IndexWriter.getInstance().insert(hdoc, accInfos[i], concurrency);

			System.out.println("********** Index Document,  Tenant[" + i + "] EXIT **********");
		}

		for (int i=0; i< 2 ; i++) {
			System.out.println("********** Get Document,  Tenant[" + i + "] ENTER **********");
			
			Doc foundDoc = IndexReader.getInstance().get(accInfos[i].name, "Key002");
			System.out.println(foundDoc.toString());
			assertEquals(foundDoc.bucketId.longValue(), buckets[i]);
			assertEquals(foundDoc.teaser.title, "Abinash Karan");

			System.out.println("********** Get Document,  Tenant[" + i + "] EXIT **********");
		}

		for (int i=0; i< 2 ; i++) {
			System.out.println("********** Search Document,  Tenant[" + i + "] ENTER **********");
			
			QueryContext ctx = new QueryContext(accInfos[i],"Abinash");
			AccountInfo accInfo = Account.getAccount(ctx.getTenant());
			for (byte[] l : accInfo.buckets) {
				System.out.println("Existing Bucket" + Storable.getLong(0, l));
			}
			
			QueryResult res = IndexReader.getInstance().search(ctx);
			assertEquals(1, res.teasers.length);
			DocTeaserWeight dtw1 = (DocTeaserWeight)res.teasers[0];
			assertEquals("Abinash Karan", dtw1.title.toString());
			System.out.println( ((DocTeaser)dtw1).toString());

			System.out.println("********** Search Document,  Tenant[" + i + "] EXIT **********");
		}

		QueryContext ctx = new QueryContext(accInfos[2],"Abinash");
		QueryResult res = IndexReader.getInstance().search(ctx);
		assertEquals(null, res.teasers);
		
		System.out.println("OK");

		IndexWriter.getInstance().delete(names[0], "Key002", concurrency);		
		IndexWriter.getInstance().delete(names[1], "Key002", concurrency);		

		System.out.println("DELETED");

	}
	
	public void testIndexWithNoKey() throws Exception {

		HDocument hdoc = new HDocument();
		hdoc.key = "SMART2";
		hdoc.tenant = ANONYMOUS;
		hdoc.title = "Abinash Karan";
		hdoc.fields = new ArrayList<Field>();
		hdoc.fields.add(new HField("LOCATION", "Bangalore"));
		IndexWriter.getInstance().insert(hdoc, acc, concurrency);
		
		QueryResult res = IndexReader.getInstance().search(
			new QueryContext(acc,"Abinash"));
		assertEquals(1, res.teasers.length);
		DocTeaserWeight dtw = (DocTeaserWeight)res.teasers[0];
		assertEquals(hdoc.title, dtw.title.toString());
		
		System.out.println("HDOC KEY = " + hdoc.key);
		IndexWriter.getInstance().delete(ANONYMOUS, hdoc.key, concurrency);		
	}	

	public void testIndexSingleDoc(String id, String term, String name, String location) throws Exception {
		index1Doc(id, term, name, location);
		IndexWriter.getInstance().delete(ANONYMOUS, id, concurrency);		
	}
	
	private void index1Doc(String id, String term, String name, String location) throws Exception {
		TermType.getInstance(true).append(
			ANONYMOUS, term, new Byte((byte) 55));

		TermType ttype = TermType.getInstance(concurrency);
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put(term, (byte) -99);
		ttype.persist(ANONYMOUS, types);

		HDocument hdoc = new HDocument();
		hdoc.key = id;
		hdoc.tenant = ANONYMOUS;
		hdoc.title = name;
		hdoc.fields = new ArrayList<Field>();
		hdoc.fields.add(new HField(term, location));
		IndexWriter.getInstance().insert(hdoc, acc, concurrency);
		
		QueryResult res = IndexReader.getInstance().search(
			new QueryContext(acc,name));
		assertEquals(1, res.teasers.length);
		DocTeaserWeight dtw = (DocTeaserWeight)res.teasers[0];
		assertEquals(id, dtw.id);
		assertEquals(name, dtw.title.toString());
	}
	
	public void testIndexSingleDocTwice(String id, String term, String name, String location) throws Exception {
		index1Doc(id, term, name, location);
		index1Doc(id, term, name, location);

		IndexWriter.getInstance().delete(ANONYMOUS, id, concurrency);		
	}
	
	public void testIndexMultiDoc(String id1,String id2,String name, String location) throws Exception {
		insert2Docs(id1,id2,name,location);
		IndexWriter.getInstance().delete(ANONYMOUS,id1, concurrency);
		IndexWriter.getInstance().delete(ANONYMOUS,id2, concurrency);
	}	
	
	public void testIndexMultidocMultiTimes(String id1,String id2,
			String name, String location) throws Exception {
		
		insert2Docs(id1, id2, name, location);
		insert2Docs(id1, id2, name, location);
		insert2Docs(id1, id2, name, location);

		IndexWriter.getInstance().delete(ANONYMOUS,id1, concurrency);
		IndexWriter.getInstance().delete(ANONYMOUS,id2, concurrency);
	}
	
	public void testIndexFieldInsert(String id, String title) throws Exception {
		
		TermType ttype = TermType.getInstance(concurrency);
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("BODY", (byte) -99);
		ttype.persist(ANONYMOUS, types);
		
		HDocument hdoc = new HDocument();
		hdoc.key = id;
		hdoc.tenant = ANONYMOUS;
		hdoc.title = title;
		hdoc.fields = new ArrayList<Field>();
		HField fld = new HField("BODY",FileReaderUtil.toString("sample.txt"));
		hdoc.fields.add(fld);
		IndexWriter.getInstance().insert(hdoc, acc, concurrency);
		
		QueryResult res = IndexReader.getInstance().search(
			new QueryContext(acc,"Comparable")); //A word from sample.txt
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		assertEquals( title, ((DocTeaser)res.teasers[0]).title);
		
		IndexWriter.getInstance().delete(ANONYMOUS,hdoc.key, concurrency);
	}
	
	public void testIndexDelete() throws Exception{
		HDocument hdoc = new HDocument();
		hdoc.key = "BIZOSYS-103";
		hdoc.tenant = ANONYMOUS;
		hdoc.title = "Ram tere Ganga maili";
		hdoc.fields = new ArrayList<Field>();
		IndexWriter.getInstance().insert(hdoc, acc, concurrency);

		QueryContext ctx1 = new QueryContext(acc,"Ganga");
		QueryResult res = IndexReader.getInstance().search(ctx1);
		assertNotNull(res);
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		
		IndexWriter.getInstance().delete(ANONYMOUS,hdoc.key, concurrency);

		QueryContext ctx2 = new QueryContext(acc,"Ganga");
		res = IndexReader.getInstance().search(ctx2);
		assertNotNull(res);
		int teasersT = ( null == res.teasers) ? 0 : res.teasers.length;
		assertEquals(teasersT, 0);
	}
	
	public void testDeleteAndGet() throws Exception {
		HDocument hdoc = new HDocument();
		hdoc.key = "BIZOSYS-103";
		hdoc.tenant = ANONYMOUS;
		hdoc.title = "Ram tere Ganga maili";
		hdoc.fields = new ArrayList<Field>();
		IndexWriter.getInstance().insert(hdoc, acc, concurrency);

		IndexWriter.getInstance().delete(this.acc.name, hdoc.key, true);
		try { 
			IndexReader.getInstance().get(this.acc.name, hdoc.key);
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			assertTrue(ex.getMessage().toLowerCase().indexOf(
				"id not found") >= 0);
		}
	}

	public void testIndexDeleteInFields() throws Exception{
		QueryResult res = null;
		TermType ttype = TermType.getInstance(concurrency);
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("emp", (byte) -98);
		types.put("role", (byte) -99);
		ttype.persist(ANONYMOUS, types);
		
		//Step 1 - Insert
		HDocument hdoc = new HDocument();
		hdoc.key = "BIZOSYS-9812";
		hdoc.tenant = ANONYMOUS;
		hdoc.fields = new ArrayList<Field>();
		hdoc.fields.add(new HField("emp", "Swami Vivenkananda"));
		hdoc.fields.add(new HField("role", "architect"));
		IndexWriter.getInstance().insert(hdoc, acc, concurrency);

		//Step 2 - Check in Dictionary
		DictionaryManager dm = DictionaryManager.getInstance();
		DictEntry de = dm.getDictionary(acc.name).get("vivenkanand");
		
		assertNotNull(de);
		//assertEquals(1, de.frequency);

		//Step 3 - Check in Index
		QueryContext ctx1 = new QueryContext(acc,"Vivenkananda");
		res = IndexReader.getInstance().search(ctx1);
		assertEquals(1, res.teasers.length);
		
		//Step 4 - Delete
		IndexWriter.getInstance().delete(ANONYMOUS,hdoc.key, concurrency);
		
		//Step 5 - Check in Index
		QueryContext ctx2 = new QueryContext(acc,"Vivenkananda");
		String pipes = "HQueryParser,"+
		"ComputeTypeCodes,QuerySequencing,SequenceProcessor," +
		"ComputeStaticRanking,CheckMetaInfo,ComputeDynamicRanking,BuildTeaser";
		res = IndexReader.getInstance().search(
			ctx2,IndexReader.getInstance().getPipes(pipes));
		assertEquals(null, res.teasers);
		
		//Step 6 - Check in Dictionary
		DictEntry de2 = DictionaryManager.getInstance().
		getDictionary(acc.name).get("vivenkanand");
		assertNotNull(de2);
		assertEquals(1, de2.frequency);
		System.out.println("testIndexDeleteInFields sucessful");
		
	}	
	

	private void insert2Docs(String id1, String id2,String name, String location) throws Exception {
		TermType ttype = TermType.getInstance(concurrency);
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("LOCATION", (byte) -99);
		ttype.persist(ANONYMOUS, types);
		
		String[] ids = new String[] {id1, id2};
		List<Field> flds = new ArrayList<Field>();
		flds.add(new HField("LOCATION", location));
		
		for ( String id : ids) {
			HDocument hdoc = new HDocument();
			hdoc.key = id ;
			hdoc.tenant = ANONYMOUS;
			hdoc.title = name;
			hdoc.fields = flds;
			IndexWriter.getInstance().insert(hdoc, acc, concurrency);
		}
		
		QueryResult res = IndexReader.getInstance().search(
			new QueryContext(acc, name));
		System.out.println(res.toString());
		assertEquals(2, res.teasers.length);
	}	
	
	public void testInsert10Docs(String id,String name, String location) throws Exception {
		
		long s = System.currentTimeMillis();
		int size = 10;
		
		List<Field> flds = new ArrayList<Field>();
		flds.add(new HField("LOCATION", location));
		
		for ( int i=0; i<size; i++) {
			HDocument hdoc = new HDocument();
			hdoc.key = id + i;
			hdoc.tenant = ANONYMOUS;
			hdoc.title = name + i;
			hdoc.fields = flds;
			IndexWriter.getInstance().insert(hdoc, acc, concurrency);
			System.out.println("Counter :" + i);
		}

		String query = "dfl:" + size + " " + location;
		System.out.println(query);
		QueryContext ctx = new QueryContext(acc, query);
		QueryResult res = IndexReader.getInstance().search(ctx);
		
		System.out.println("Total Time Taken :" + (System.currentTimeMillis() - s) );
		List<String> deletes = new ArrayList<String>(size);
		for ( int i=0; i<size; i++) {
			deletes.add(id + i);
		}
		IndexWriter.getInstance().delete(acc.name, deletes);
		assertEquals(size, res.teasers.length);
	}		
		
	public void insert2DocsIn2BucketsWithSameSerial() throws Exception {
		
		String[] ids = new String[] {"001", "002"};
		long[] buckets = null;
		buckets = ( acc.buckets.size() == 1) ?
			new long[] {Account.getCurrentBucket(acc), Account.getNextBucket(acc)} :
			new long[] { Storable.getLong(0, acc.buckets.get(0)), Storable.getLong(0, acc.buckets.get(1))};	
		String[] locations = new String[] {"bangalore", "keonjhar"};

		for ( int i=0; i< 2; i++) {
			HDocument hdoc = new HDocument();
			hdoc.key = ids[i] ;
			hdoc.bucketId = buckets[i];
			hdoc.tenant = ANONYMOUS;
			hdoc.docSerialId = Short.MAX_VALUE;
			hdoc.title = "Document For " + 
				hdoc.key + "/" + hdoc.bucketId + " " + hdoc.docSerialId;
			hdoc.fields = new ArrayList<Field>();
			hdoc.fields.add(new HField("loc", locations[i]));
			IndexWriter.getInstance().insert(hdoc, acc, concurrency);
		}
		
		QueryResult res = IndexReader.getInstance().search(
			new QueryContext(acc, "bangalore"));
		assertEquals(1, res.teasers.length);
		
		for ( int i=0; i< 2; i++) {
			IndexWriter.getInstance().delete(ANONYMOUS, ids[i] , concurrency);
		}
		
	}
	
	public void testInsertBatch(String id,String name, String location) throws Exception {
		long s = System.currentTimeMillis();
		int size = 50000;
		List<HDocument> docs = new ArrayList<HDocument>(1024);
		int cache = 0;
		for ( int i=0; i<size; i++) {
			cache++;
			HDocument hdoc = new HDocument();
			hdoc.key = id + i;
			hdoc.tenant = ANONYMOUS;
			hdoc.title = name + i;
			hdoc.fields = new ArrayList<Field>();
			hdoc.fields.add(new HField("LOCATION", location));
			docs.add(hdoc);
			if ( cache >= 256) {
				IndexWriter.getInstance().insertBatch(docs, acc, concurrency);
				cache = 0;
				docs.clear();
			}
		}
		IndexWriter.getInstance().insertBatch(docs, acc, concurrency);
		if ( 1 ==1 ) return;
		long e= System.currentTimeMillis();		
		System.out.println("Batch Insert Time Taken :" + (e - s) );
		s = e;

		String query = "dfl:1000 " + location;
		if (  1 == 1) {
			System.out.println(query);
			QueryContext ctx = new QueryContext(acc, query);
			QueryResult res = IndexReader.getInstance().search(ctx);
			e= System.currentTimeMillis();
			System.out.println("Query Time Taken :" + (e - s) );
			s = e; 		
			assertEquals(size, res.teasers.length);
		}
		
		if ( 1 ==1 ) return;
		List<String> deletes = new ArrayList<String>();
		for ( int i=0; i<size; i++) {
			deletes.add(id + i);
		}
		IndexWriter.getInstance().delete(acc.name, deletes);
		e= System.currentTimeMillis();
		System.out.println("Batch delete Time Taken :" + (e - s) );
		s = e; 		

		if (  1 == 1) {
			QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc, query));
			System.out.println("Query Time Taken :" + (System.currentTimeMillis() - s) );
			int found = ( null == res2.teasers) ? 0 : res2.teasers.length; 
			assertEquals(0, found);
		}
		printInvertedIndex();
	}		
	
	public void testIndexUpdateField(String keyword1, String keyword2, String keyword3, 
			String keyword4, String keyword5, String keyword6, String keyword7,  
			String keyword8, String keyword9, String keyword10) throws Exception {
		
		String[] keywords = new String[] {
				keyword1, keyword2, keyword3, keyword4, keyword5,
				keyword6, keyword7, keyword8, keyword9, keyword10
		};
		
		//Write the same documents various time with different keywords
		String key = "KEY00456";
		for ( int i=0; i<10; i++) {
			HDocument hdoc = new HDocument();
			hdoc.key = key;
			hdoc.tenant = ANONYMOUS;
			hdoc.fields = new ArrayList<Field>();
			HField fld = new HField("fld1", keywords[i]);
			hdoc.fields.add(fld);
			IndexWriter.getInstance().insert(hdoc, acc, concurrency);

			String query = keywords[i];
			QueryContext ctx = new QueryContext(acc, query);
			QueryResult res = IndexReader.getInstance().search(ctx);
			assertEquals(1, res.teasers.length);
		}
		
		for ( int i=0; i<10; i++) {
			String query = keywords[i];
			QueryContext ctx = new QueryContext(acc, query);
			QueryResult res = IndexReader.getInstance().search(ctx);
			int size = ( null == res.teasers) ? 0 : res.teasers.length;
			if ( 9 == i) assertEquals(1, size);
			else assertEquals(0, size);
		}
		
		IndexWriter.getInstance().delete(ANONYMOUS,key, concurrency);
		
	}
		
	public void testIndexUpdateSocialText(String keyword1, String keyword2, String keyword3, 
			String keyword4, String keyword5, String keyword6, String keyword7,  
			String keyword8, String keyword9, String keyword10) throws Exception {
		
		String[] keywords = new String[] {
				keyword1, keyword2, keyword3, keyword4, keyword5,
				keyword6, keyword7, keyword8, keyword9, keyword10
		};
		
		for (int i=0; i<10; i++) {
			keywords[i] = keywords[i].replace(" ", "");
			keywords[i] = keywords[i].replace("-", "");
			keywords[i] = keywords[i].replace("_", "");
		}
		
		//Write the same documents various time with different keywords
		String key = "KEY00456";
		for ( int i=0; i<10; i++) {
			HDocument hdoc = new HDocument();
			hdoc.key = key;
			hdoc.tenant = ANONYMOUS;
			hdoc.socialText = new ArrayList<String>();
			hdoc.socialText.add(keywords[i]);
			IndexWriter.getInstance().insert(hdoc, acc, concurrency);

			String query = keywords[i];
			QueryContext ctx = new QueryContext(acc, query);
			QueryResult res = IndexReader.getInstance().search(ctx);
			assertEquals(1, res.teasers.length);
		}
		
		for ( int i=0; i<10; i++) {
			String keyword = keywords[i];
			QueryContext ctx = new QueryContext(acc, keyword);
			QueryResult res = IndexReader.getInstance().search(ctx);
			int size = ( null == res.teasers) ? 0 : res.teasers.length;
			DictEntry entry = DictionaryManager.getInstance().get(
				acc.name, Stemmer.getInstance().stem(keyword.toLowerCase()));
			int entryF = ( null == entry) ? 0 : entry.frequency;
			
			if ( 9 == i) {
				assertEquals(1, size);
				assertEquals(1, entry.frequency);
			} else {
				assertEquals(0, size);
				assertEquals(0, entryF);
			}
			
		}
		
		IndexWriter.getInstance().delete(ANONYMOUS,key, concurrency);
	}	
	
	/**
	private void testLoadCsvWaterLevel() throws Exception {
		
		HDocument pristineDoc = new HDocument();
		URL url = new URL("file:///d:/delme/water level.csv");
	    int [] columnFormats = new int[]{
	    		DataLoader.NONE,DataLoader.NONE,DataLoader.NEUMERIC,
	    		DataLoader.NEUMERIC,DataLoader.NONE,
	    		DataLoader.DECIMAL,DataLoader.DECIMAL,DataLoader.DECIMAL,
	    		DataLoader.DECIMAL,DataLoader.DECIMAL,DataLoader.DECIMAL,
	    		DataLoader.DECIMAL,DataLoader.DECIMAL,DataLoader.DECIMAL,
	    		DataLoader.DECIMAL,DataLoader.DECIMAL,DataLoader.DECIMAL,
	    		DataLoader.DECIMAL};

	    int [] nonEmptyCells = new int[]{0,1,2};

	    int [] optionCheckCells = new int[]{2};
	    String optionCheckValues[] = {"2005,2006,2007,2008,2009,2010,2011,2012"};


	    int [] minCheckCells = new int[]{2,5,6,7,8,9,10,11,12,13,14,15,16,17};
	    double[] minCheckValues = new double[] {2005,-10,-10,-10,-10,-10,-10,-10,-10,-10,-10,-10,-10,-10};

	    int [] maxCheckCells = new int[]{2,5,6,7,8,9,10,11,12,13,14,15,16,17};
	    double[] maxCheckValues = new double[] {2012,500,500,500,500,500,500,500,500,500,500,500,500,500};

		RowEventProcessor handler = new RowEventProcessorHSearch(
				this.acc, pristineDoc, null,
				"wlvl-", -1, -1,
				new int[]{0,1,2,3}, -1,
				new int[]{0,1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17},
				"waterlevel", new int[]{0,1,2,3},
				0,-1,256, false, null);
	    
	    DataLoader.load(url, true, handler, "," ,
	    	columnFormats, nonEmptyCells, optionCheckCells, 
	    	optionCheckValues, minCheckCells, minCheckValues, 
	    	maxCheckCells, maxCheckValues);
	}	
	
	
	private void testLoadCsvHabitation() throws Exception {
		
		HDocument pristineDoc = new HDocument();
		URL url = new URL("file:///d:/delme/habitation.csv");
	    int [] columnFormats = new int[]{
	    		DataLoader.NONE,DataLoader.NONE,DataLoader.NONE,DataLoader.NONE,
	    		DataLoader.NONE,DataLoader.NONE,DataLoader.NONE,
	    		DataLoader.DECIMAL,DataLoader.DECIMAL,DataLoader.DECIMAL,
	    		DataLoader.DECIMAL,DataLoader.DECIMAL,DataLoader.DECIMAL,
	    		DataLoader.DECIMAL,DataLoader.DECIMAL,DataLoader.ALPHA};

	    int [] nonEmptyCells = new int[]{0,1,2,3,4,5};

	    int [] optionCheckCells = new int[]{0};
	    String optionCheckValues[] = {"2011-2012"};


		RowEventProcessor handler = new RowEventProcessorHSearch(
				this.acc, pristineDoc, null,
				"hab-", -1, -1,
				new int[]{0,1,2,3,4,5,6,7,8}, -1,
				new int[]{0,1,2,4,5,6,7,8,9,10,11,12,13,14,15,16},
				"habitation", new int[]{0,1,2,3,4,5,6},
				0,-1,256, false, null);
	    
	    DataLoader.load(url, true, handler, "," ,
	    	columnFormats, nonEmptyCells, optionCheckCells, 
	    	optionCheckValues, null, null, null, null);
	}
	*/	
	
	public void testFastTruncate() throws Exception {
		try {
			IndexWriter.getInstance().truncate("main");
		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}
	
}
