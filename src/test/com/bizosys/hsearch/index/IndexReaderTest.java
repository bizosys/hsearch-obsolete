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

import java.io.StringWriter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import org.apache.commons.lang.StringEscapeUtils;

import com.bizosys.hsearch.TestAll;
import com.bizosys.hsearch.common.AccessDefn;
import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Field;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.HField;
import com.bizosys.hsearch.common.SField;
import com.bizosys.hsearch.common.WhoAmI;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.inpipe.util.StopwordManager;
import com.bizosys.hsearch.inpipe.util.StopwordRefresh;
import com.bizosys.hsearch.query.DocTeaserWeight;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.ServiceFactory;
import com.bizosys.oneline.util.StringUtils;

public class IndexReaderTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[1];  
	
	public static void main(String[] args) throws Exception {
		IndexReaderTest t = new IndexReaderTest();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.tryOut();
			t.tearDown();
		}
	}

	boolean isMultiClient = true;
	AccountInfo acc = null;
	private String ANONYMOUS = null;
	
	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);

		this.ANONYMOUS = Thread.currentThread().getName();
		this.acc = Account.getAccount(ANONYMOUS);
		if ( null == acc) {
			acc = new AccountInfo(ANONYMOUS);
			acc.name = ANONYMOUS;
			acc.maxbuckets = 5;
			Account.storeAccount(acc);
		} else {
			//if ( ! mode.equals(modes[2])) IndexWriter.getInstance().truncate(ANONYMOUS);
		}
		
	}
	
	@Override
	protected void tearDown() throws Exception {
		ServiceFactory.getInstance().stop();
	}
	
	private void tryOut() throws Exception{
		Scanner in = new Scanner(System.in);
		String query = in.nextLine();
		in.close();

		QueryContext ctx = new QueryContext(acc, query);
		
		QueryResult results = null;
		results = IndexReader.getInstance().search(ctx);
		
		int size = ( null == results) ? 0 : 
			( null == results.teasers) ? 0 : results.teasers.length;
		if ( 0 == size) {
			System.out.println("<list></list>");
			return;
		}
		
		System.out.println(results.toString());
		System.out.println( IndexReader.getInstance().get(acc.name, "6EY-VF9U5uQ") );
		
	}
	
	public void testGet(String title, String content, String id ) throws Exception {
		if ( null != title ) title = title.trim();
		if ( null != content ) content = content.trim();
		if ( null != id ) id = id.trim();
		
		HDocument doc1 = new HDocument();
		doc1.key = id;
		doc1.title = "Title : " + title ;
		doc1.tenant = ANONYMOUS;
		doc1.cacheText = doc1.title + " " + content;
		
		doc1.citationTo = new ArrayList<String>();
		doc1.citationTo.add("google");

		doc1.citationFrom = new ArrayList<String>();
		doc1.citationFrom.add("bizosyus");

		doc1.fields = new ArrayList<Field>();
		HField fld = new HField("BODY", content);
		doc1.fields.add(fld);
		
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		Doc d = IndexReader.getInstance().get(ANONYMOUS, id);
		d.acl.editPermission = new Access();
		d.acl.editPermission.addAnonymous();
		
		assertNotNull(d);
		assertEquals(d.teaser.id, doc1.key);
		assertEquals(d.teaser.title, doc1.title);
		assertEquals(d.teaser.cacheText, doc1.cacheText);
		//assertEquals(d.content.stored.size(), 1);
		assertEquals(d.content.citationTo.size(), 1);
		assertEquals(new String((byte[])d.content.citationTo.get(0)), "google");
		
		StringWriter s = new StringWriter();
		d.toXml(s);
		System.out.println(s);

		IndexWriter.getInstance().delete(ANONYMOUS, id,isMultiClient);
	}
	
	public void testVanillaSearch(String title) throws Exception  {
		String id = "ID002";
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id : " + id ;
		doc1.title = "Title : " + title;
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		QueryResult res = null;
		try {
			QueryContext ctx = new QueryContext(acc, title);
			res = IndexReader.getInstance().search(ctx);
		} catch (ApplicationFault ex) {
			if ( ex.getMessage().equals("Null query")) return;
			else throw ex;
		}
		
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight teaser = (DocTeaserWeight) res.teasers[0];
		assertEquals(teaser.id, doc1.key);
		assertEquals(teaser.title, doc1.title);
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}

	public void testNullWord() throws Exception  {
		try {
			IndexReader.getInstance().search(null);
		} catch (ApplicationFault ex) {
			assertEquals("Blank Query", ex.getMessage().trim());
		}
	}
	
	public void test2CharacterWord() throws Exception  {
		String id = "ID003";

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id : " + id ;
		doc1.title = "Title : " + "The Sun God RA was worshipped by Egyptians.";
		doc1.loadBucketAndSerials(acc);
		
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		QueryContext ctx = new QueryContext(acc,  "ra");
		
		QueryResult res = IndexReader.getInstance().search(ctx);
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight teaser = (DocTeaserWeight) res.teasers[0];
		assertEquals(teaser.id, doc1.key);
		assertEquals(teaser.title, doc1.title);
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testSpecialCharacter() throws Exception  {

		String id = "ID004";
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id : " + id ;
		doc1.title = "For the Sin!1 city I will design wines & wives";
		doc1.loadBucketAndSerials(acc);
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		QueryContext ctx = new QueryContext(acc, "wines & wives");
		QueryResult res = IndexReader.getInstance().search(ctx);
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight teaser = (DocTeaserWeight) res.teasers[0];
		assertEquals(teaser.id, doc1.key);
		assertEquals(teaser.title, StringEscapeUtils.escapeXml(doc1.title));
		
		ctx = new QueryContext(acc, "sin!1");
		res = IndexReader.getInstance().search(ctx);
		assertTrue(res.teasers.length > 0);
		String allIds = "";
		for ( Object stwO : res.teasers) {
			teaser = (DocTeaserWeight) stwO;
			allIds = teaser.id + allIds; 
		}
		assertTrue(allIds.indexOf(doc1.key) >= 0);
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		System.out.println("testSpecialCharacter DONE");
	}
	
	
	public void testDocumentType() throws Exception  {
		String id = "ID005";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("employee", (byte) -113);
		dtype.persist(ANONYMOUS, types);
		
		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("empid", (byte) -100);
		tt.put("name", (byte) -99);
		ttype.persist(ANONYMOUS, tt);
		Thread.sleep(50);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		HField fld1 = new HField("empid", "5183");
		HField fld2 = new HField("name", "Abinash Karan");
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(fld1);
		doc1.fields.add(fld2);
		doc1.docType = "employee";
		doc1.loadBucketAndSerials(acc);
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		
		HDocument doc2 = new HDocument(ANONYMOUS);
		doc2.key = "Id 2 : " + id ;
		HField fld3 = new HField("empid", "5184");
		HField fld4 = new HField("name", "Abinash Bagha");
		doc2.fields = new ArrayList<Field>();
		doc2.fields.add(fld3);
		doc2.fields.add(fld4);
		doc2.loadBucketAndSerials(acc);
		IndexWriter.getInstance().insert(doc2, acc, isMultiClient);

		HDocument doc3 = new HDocument(ANONYMOUS);
		doc3.key = "Id 3 : " + id ;
		HField fld5 = new HField("empid", "5185");
		HField fld6 = new HField("name", "Abinash Mohanty");
		doc3.fields = new ArrayList<Field>();
		doc3.fields.add(fld5);
		doc3.fields.add(fld6);
		doc3.loadBucketAndSerials(acc);
		IndexWriter.getInstance().insert(doc3, acc, isMultiClient);

		QueryResult res = IndexReader.getInstance().search(
			new QueryContext(acc, "typ:employee abinash"));
		assertNotNull(res.teasers);
		assertEquals(1, res.teasers.length);
		DocTeaserWeight t1 = (DocTeaserWeight) res.teasers[0];
		String id1 = t1.id;

		assertTrue( -1 != id1.indexOf(doc1.key) );
		
		IndexWriter.getInstance().delete(ANONYMOUS,doc1.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS,doc2.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS,doc3.key,isMultiClient);
		
		
	}
	
	public void testAbsentDocType() throws Exception  {
		String id = "ID006";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("toys", (byte) -109);
		dtype.persist(ANONYMOUS, types);
		
		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("creative", (byte) -98);
		tt.put("name", (byte) -99);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		HField fld1 = new HField("creative", "dow");
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(fld1);
		doc1.docType = "toys";
		doc1.loadBucketAndSerials(acc);
		IndexWriter.getInstance().insert(doc1,  acc, isMultiClient);
		
		QueryResult res1 = IndexReader.getInstance().search(
			new QueryContext(acc, "typ:toys dow"));
		if ( 1 != res1.teasers.length) System.out.println(res1.toString());
		assertEquals(1, res1.teasers.length);
		
		try { 
			IndexReader.getInstance().search(
				new QueryContext(acc, "typ:gal dow"));
		} catch (ApplicationFault ex) {
			String err = ex.getMessage().toLowerCase();
			assertTrue(err.indexOf("unknown") >= 0);
		} finally {
			IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		}
	}
	
	public void testTermType() throws Exception  {
		
		String id = "ID007";
		
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.cacheText = null;
		doc1.key = "Id 1 : " + id ;
		Field fld1 = new SField("gas", "Hydrogen");
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(fld1);
		doc1.docType = "molecules";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		HDocument doc2 = new HDocument(ANONYMOUS);
		doc2.key = "Id 2 : " + id ;
		doc2.title = "Water is made of hydrogen and oxygen.";
		IndexWriter.getInstance().insert(doc2, acc, isMultiClient);
		
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc,"Hydrogen"));
		assertNotNull(res1.teasers);
		assertEquals(2, res1.teasers.length);
			
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext(acc,"gas:Hydrogen"));
		assertEquals(1, res2.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS, doc2.key,isMultiClient);
	}
	
	public void testAbsentTermType() throws Exception  {
		String id = "ID008";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("stories", (byte) -87);
		dtype.persist(ANONYMOUS, types);
		
		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("character", (byte) -121);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		HField fld1 = new HField("character", "cyndrella");
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(fld1);
		doc1.docType = "stories";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		
		try { 
			IndexReader.getInstance().search(new QueryContext(acc,"girl:cyndrella"));
		} catch (ApplicationFault ex) {
			String err = ex.getMessage().toLowerCase();
			assertTrue(err.indexOf("unknown") >= 0);
		} finally {
			IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		}
	}
	
	public void testDocumentTypeWithTermType() throws Exception  {
		String id = "ID009";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("molecules", (byte) -108);
		types.put("fuel", (byte) -100);
		dtype.persist(ANONYMOUS, types);

		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("gas", (byte) -97);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		HField fld1 = new HField("gas", "Hydrogen");
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(fld1);
		doc1.docType = "molecules";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		HDocument doc2 = new HDocument(ANONYMOUS);
		doc2.key = "Id 2 : " + id ;
		HField fld2 = new HField("gas", "Hydrogen");
		doc2.fields = new ArrayList<Field>();
		doc2.fields.add(fld2);
		doc2.docType = "fuel";
		IndexWriter.getInstance().insert(doc2, acc, isMultiClient);

		QueryResult res1 = IndexReader.getInstance().search(
			new QueryContext(acc,"gas:Hydrogen typ:molecules"));
		System.out.println(res1.toString());
		assertNotNull(res1.teasers);
		assertEquals(1, res1.teasers.length);
		String f1 = ((DocTeaserWeight)res1.teasers[0]).id;
		assertEquals(doc1.key, f1);
		
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext(acc,"gas:Hydrogen typ:fuel"));
		System.out.println("RES2>>>>" + res2.toString());
		assertEquals(1, res2.teasers.length);
		String f2 = ((DocTeaserWeight)res2.teasers[0]).id;
		assertEquals(doc2.key, f2);

		QueryResult res3 = IndexReader.getInstance().search(new QueryContext(acc,"gas:Hydrogen"));
		System.out.println("RES3>>>>" + res3.toString());
		assertEquals(2, res3.teasers.length);
		String f12 = f1 + f2;
		assertTrue(f12.indexOf(doc1.key) != -1 );
		assertTrue(f12.indexOf(doc2.key) != -1 );

		IndexWriter.getInstance().delete(ANONYMOUS, doc2.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testAnd() throws Exception  {
		String id = "ID010";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("technology", (byte) -108);
		dtype.persist(ANONYMOUS, types);

		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("lang", (byte) -97);
		tt.put("os", (byte) -98);
		tt.put("middleware", (byte) -99);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(new HField("lang", "Java"));
		doc1.fields.add(new HField("os", "linux"));
		doc1.fields.add(new HField("middleware", "weblogic"));
		doc1.docType = "technology";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		
		HDocument doc2 = new HDocument(ANONYMOUS);
		doc2.key = "Id 2 : " + id ;
		doc2.fields = new ArrayList<Field>();
		doc2.fields.add(new HField("lang", "Java"));
		doc2.fields.add(new HField("os", "windows"));
		doc2.fields.add(new HField("middleware", "weblogic"));
		doc2.docType = "technology";
		IndexWriter.getInstance().insert(doc2, acc, isMultiClient);
		
		HDocument doc3 = new HDocument(ANONYMOUS);
		doc3.key = "Id 3 : " + id ;
		doc3.fields = new ArrayList<Field>();
		doc3.fields.add(new HField("lang", "Java"));
		doc3.fields.add(new HField("os", "linux"));
		doc3.fields.add(new HField("middleware", "hadoop"));
		doc3.docType = "technology";
		IndexWriter.getInstance().insert(doc3, acc, isMultiClient);		

		QueryResult res1 = IndexReader.getInstance().search(new QueryContext(acc,"+java +linux"));
		//System.out.println("RES1>>>>" + res1.toString());
		assertEquals(2, res1.teasers.length);
		String f12 = ((DocTeaserWeight)res1.teasers[0]).id + 
			((DocTeaserWeight)res1.teasers[1]).id;
		assertTrue(f12.indexOf(doc1.key) != -1 );
		assertTrue(f12.indexOf(doc3.key) != -1 );
		
		QueryContext AND = new QueryContext(acc,"java AND weblogic");
		System.out.println("AND>>>>" + AND.toString());
		QueryResult res2 = IndexReader.getInstance().search(AND);
		//System.out.println("RES2>>>>" + res2.toString());
		assertEquals(2, res2.teasers.length);
		f12 = ((DocTeaserWeight)res2.teasers[0]).id + 
			((DocTeaserWeight)res2.teasers[1]).id;
		assertTrue(f12.indexOf(doc1.key) != -1 );
		assertTrue(f12.indexOf(doc2.key) != -1 );

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS, doc2.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS, doc3.key,isMultiClient);
	}
	
	public void testOr() throws Exception  {

		String id = "ID011";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("fruit", (byte) -108);
		dtype.persist(ANONYMOUS, types);

		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("name", (byte) -97);
		tt.put("season", (byte) -98);
		tt.put("price", (byte) -99);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(new HField("name", "Mango"));
		doc1.fields.add(new HField("season", "summer"));
		doc1.fields.add(new HField("price", "Rs70/-"));
		doc1.docType = "fruit";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		
		HDocument doc2 = new HDocument(ANONYMOUS);
		doc2.key = "Id 2 : " + id ;
		doc2.fields = new ArrayList<Field>();
		doc2.fields.add(new HField("name", "Banana"));
		doc2.fields.add(new HField("season", "Any"));
		doc2.fields.add(new HField("price", "Rs28/-"));
		doc2.docType = "fruit";
		IndexWriter.getInstance().insert(doc2, acc, isMultiClient);
		
		HDocument doc3 = new HDocument(ANONYMOUS);
		doc3.key = "Id 3 : " + id ;
		doc3.fields = new ArrayList<Field>();
		doc3.fields.add(new HField("name", "Watermelon"));
		doc3.fields.add(new HField("season", "Summer"));
		doc3.fields.add(new HField("price", "Rs70/-"));
		doc3.docType = "fruit";
		IndexWriter.getInstance().insert(doc3, acc, isMultiClient);		

		QueryResult res1 = IndexReader.getInstance().search(new QueryContext(acc,"Summer Banana"));
		System.out.println("RES1>>>>" + res1.toString());
		assertEquals(3, res1.teasers.length);
		String f123 = ((DocTeaserWeight)res1.teasers[0]).id +  
			((DocTeaserWeight)res1.teasers[1]).id  + 
			((DocTeaserWeight)res1.teasers[2]).id;
		
		assertTrue(f123.indexOf(doc1.key) != -1 );
		assertTrue(f123.indexOf(doc2.key) != -1 );
		assertTrue(f123.indexOf(doc3.key) != -1 );
		
		QueryResult res2 = IndexReader.getInstance().search(new QueryContext(acc,"summer OR Rs70/-"));
		System.out.println("RES2>>>>" + res2.toString());
		assertEquals(2, res2.teasers.length);
		String f12 = ((DocTeaserWeight)res2.teasers[0]).id + 
			((DocTeaserWeight)res2.teasers[1]).id;
		assertTrue(f12.indexOf(doc1.key) != -1 );
		assertTrue(f12.indexOf(doc3.key) != -1 );

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS, doc2.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS, doc3.key,isMultiClient);		
	}
	
	public void testMultiField() throws Exception{
		String id = "FREEBASE44";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("location", (byte) -108);
		dtype.persist(ANONYMOUS, types);

		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("name", (byte) -97);
		tt.put("geolocation", (byte) -98);
		tt.put("containedby", (byte) -99);
		tt.put("area", (byte) -100);
		tt.put("time_zones", (byte) -101);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(new HField("name", "pine hill"));
		doc1.fields.add(new HField("geolocation", "/guid/9202a8c04000641f800000000116a5a2"));
		doc1.fields.add(new HField("containedby", "camden county,new jersey"));
		doc1.fields.add(new HField("area", "10.3599524413"));
		doc1.fields.add(new HField("time_zones", "north american eastern time zone"));
		doc1.docType = "location";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		
		QueryResult res1 = IndexReader.getInstance().search(new QueryContext(acc,"jersey"));
		System.out.println("RES2>>>>" + res1.toString());

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		
	}
	
	public void testMultiphrase() throws Exception  {
		String id = "ID012";

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = id;
		doc1.title = "I born at Keonjhar Orissa";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		QueryResult res1 = IndexReader.getInstance().search(new QueryContext(acc,"Keonjhar Orissa"));
		assertTrue(((DocTeaserWeight)res1.teasers[0]).id.indexOf(doc1.key) != -1 );

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testQuotedMultiphrase() throws Exception  {
		String id = "ID013";
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = id;
		doc1.title = "Oriya is my mother toungh. I do lot of spelling mistakes in english.";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		QueryResult res1 = IndexReader.getInstance().search(new QueryContext(acc,"\"mother toungh\""));
		assertTrue(((DocTeaserWeight)res1.teasers[0]).id.indexOf(doc1.key) != -1 );

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testStopword() throws Exception  {
		String id = "ID014";
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = id;
		List<String> stopwords = new ArrayList<String> ();
		stopwords.add("a");
		stopwords.add("and");
		StopwordManager.getInstance().setStopwords(stopwords);
		new StopwordRefresh().process(); //A 30mins job is done in sync mode

		doc1.title = "Once upon a time a tiger and a horse were staying together.";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		try {
			IndexReader.getInstance().search(new QueryContext(acc,"and"));
		} catch (Exception ex) {
			assertTrue(ex.getMessage().indexOf("No search query present") >= 0);
		}
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testMultiphraseWhereOneIsStopWord() throws Exception  {
		String id = "ID015";
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = id;
		List<String> stopwords = new ArrayList<String> ();
		stopwords.add("a");
		stopwords.add("and");
		StopwordManager.getInstance().setStopwords(stopwords);
		new StopwordRefresh().process(); //A 30mins job is done in sync mode

		doc1.title = "Once upon a time a tiger and a crocodile were staying together.";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		QueryResult res = IndexReader.getInstance().search(new QueryContext(acc,"and a crocodile"));
		assertNotNull(res);
		assertEquals(1, res.teasers.length);
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testTypeAndNonTypeMixed() throws Exception  {
		String id = "ID016";

		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("fruit", (byte) -108);
		dtype.persist(ANONYMOUS, types);
		
		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("name", (byte) -97);
		tt.put("price", (byte) -99);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(new HField("name", "apple"));
		doc1.fields.add(new HField("price", "123.00"));
		doc1.docType = "fruit";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		QueryResult res = IndexReader.getInstance().search(new QueryContext(acc,"+name:apple +123.00"));
		assertNotNull(res);
		assertEquals(1, res.teasers.length);
		String f1 = ((DocTeaserWeight)res.teasers[0]).id;
		assertTrue(f1.indexOf(doc1.key) != -1 );

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testTypeNonTypeAndWrongType() throws Exception  {
		String id = "ID918";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("baby", (byte) -108);
		dtype.persist(ANONYMOUS, types);

		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("babyname", (byte) -97);
		tt.put("house", (byte) -99);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(new HField("babyname", "Ava"));
		doc1.fields.add(new HField("house", "466"));
		doc1.docType = "baby";
		
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		System.out.println("Checkig wrong Type");
		try {
			IndexReader.getInstance().search(
				new QueryContext(acc, "+XX:Ava +466"));
		} catch (Exception ex) {
			String err = ex.getMessage().toLowerCase();
			assertTrue(err.indexOf("unknown") >= 0);
		} 
		
		System.out.println("Checkig TypeNonType");
		QueryResult res3 = IndexReader.getInstance().search(
			new QueryContext(acc, "+babyname:Ava 466"));
		System.out.println( res3.toString() );
		assertEquals(1, res3.teasers.length);
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testNonExistance() throws Exception  {
		String id = "ID017";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("baby", (byte) -108);
		dtype.persist(ANONYMOUS, types);

		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("babyname", (byte) -97);
		tt.put("house", (byte) -99);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(new HField("babyname", "Ava"));
		doc1.fields.add(new HField("house", "466"));
		doc1.docType = "baby";
		
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		System.out.println("Checkig Type and Wrong Non Type");
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc, "+babyname:Ava +jhumritalya"));
		int total = ( null == res1.teasers) ? 0 : res1.teasers.length; 
		assertEquals(total, 0);
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}	

	public void testDocumentStateFilter() throws Exception  {
		String id = "ID018";
		
		DocumentType dtype = DocumentType.getInstance();
		Map<String, Byte> types = new HashMap<String, Byte>();
		types.put("leave", (byte) -108);
		dtype.persist(ANONYMOUS, types);

		TermType ttype = TermType.getInstance(isMultiClient);
		Map<String, Byte> tt = new HashMap<String, Byte>();
		tt.put("fromdate", (byte) -97);
		ttype.persist(ANONYMOUS, tt);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.fields = new ArrayList<Field>();
		doc1.fields.add(new HField("fromdate", "12thDec,2009"));
		doc1.docType = "leave";
		doc1.state =  "closed";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		QueryResult res1 = IndexReader.getInstance().search(
			new QueryContext(acc,"12thDec,2009"));
		assertEquals(1, res1.teasers.length);
		
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc,"ste:_active 12thDec,2009"));
			assertEquals(0, res2.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		
	}
	
	public void testTeam() throws Exception  {

		String id = "ID019";
		
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.title = "contactus In Call center";
		doc1.team = "icici";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		
		HDocument doc2 = new HDocument(ANONYMOUS);
		doc2.key = "Id 2 : " + id ;
		doc2.title = "contactus Offshore";
		doc2.docType = "leave";
		doc2.team = "infosys";
		IndexWriter.getInstance().insert(doc2, acc, isMultiClient);

		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc,"team:icici contactus"));
		assertEquals(1, res1.teasers.length);
		String f1 = ((DocTeaserWeight)res1.teasers[0]).id;
		assertEquals(doc1.key, f1);
			
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc,"contactus"));
		assertEquals(2, res2.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
		IndexWriter.getInstance().delete(ANONYMOUS, doc2.key,isMultiClient);
		
	}
	
	public void testCreatedBefore() throws Exception  {
		String id = "ID020";

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.title = "Born at poland around 1971" + 
			"Fri, 1 Jan 1971 00:00:00 UTC";
		
		doc1.createdOn = new Date(31536000L);
		System.out.println("Date conversion :" + doc1.createdOn.toString());
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		Date date = new Date();
		String nowDate = new Long(date.getTime()).toString();
		String query = "createdb:" + nowDate + " poland";
		System.out.println("Query :" + query);

		QueryResult res = IndexReader.getInstance().search(
				new QueryContext(acc, query));
		assertEquals(1, res.teasers.length);
		
		String pastDate = "100";
		String pastQ = "createdb:" + pastDate + " century";
		QueryResult pastRes = IndexReader.getInstance().search(
				new QueryContext(acc, pastQ));
		int size = ( null == pastRes) ? 0 : 
			( null == pastRes.teasers) ? 0 : pastRes.teasers.length; 
		assertEquals(0, size);

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}

	public void testCreatedAfter() throws Exception  {
		String id = "ID021";

		DateFormat format = DateFormat.getDateTimeInstance(
            DateFormat.MEDIUM, DateFormat.SHORT);
		
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.title = "My daughter birth was after my birth";
		doc1.createdOn = format.parse("Nov 18, 2008 6:15 AM");
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		String myBirth = new Long(format.parse("Feb 05, 1977 8:00 PM").getTime()).toString();
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc,"createda:" + myBirth + " birth"));
		assertEquals(1, res1.teasers.length);
		
		String toDate = new Long(new Date().getTime()).toString();
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc,"createda:" + toDate + " birth"));
		assertEquals(0, res2.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testModifiedAfter() throws Exception  {
		String id = "ID022";
		
		DateFormat format = DateFormat.getDateTimeInstance(
	            DateFormat.MEDIUM, DateFormat.SHORT);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.title = "My Trading balance as 234.00";
		doc1.modifiedOn = new Date();
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		String myBirth = new Long(format.parse("Feb 05, 1977 8:00 PM").getTime()).toString();
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc,"modifieda:" + myBirth + " balance"));
		assertEquals(1, res1.teasers.length);
			
		String future = new Long(format.parse("Feb 05, 2121 8:00 PM").getTime()).toString();
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc,"modifieda:" + future + " balance"));
		assertEquals(0, res2.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testModifiedBefore() throws Exception  {
		String id = "ID023";
		
		DateFormat format = DateFormat.getDateTimeInstance(
	            DateFormat.MEDIUM, DateFormat.SHORT);

		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "Id 1 : " + id ;
		doc1.title = "My Trading balance as 234.00";
		doc1.modifiedOn = new Date();
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);

		String myBirth = new Long(format.parse("Feb 05, 1977 8:00 PM").getTime()).toString();
		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc,"modifiedb:" + myBirth + " balance"));
		assertEquals(0, res1.teasers.length);
			
		String future = new Long(format.parse("Feb 05, 2121 8:00 PM").getTime()).toString();
		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc,"modifiedb:" + future + " balance"));
		assertEquals(1, res2.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc1.key,isMultiClient);
	}
	
	public void testDocumentFetchLimit() throws Exception  {
		String id = "ID024";

		ArrayList<HDocument> docs = new ArrayList<HDocument>();
		for ( int i=0; i<10; i++) {
			HDocument doc = new HDocument(ANONYMOUS);
			doc.key = i + " - " + id ;
			doc.title = "Flower " + i;
			docs.add(doc);
		}
		IndexWriter.getInstance().insertBatch(docs,acc, isMultiClient);

		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc,"dfl:3 flower"));
		assertEquals(3, res1.teasers.length);

		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc,"dfl:1 flower"));
		assertEquals(1, res2.teasers.length);
		
		QueryResult res3 = IndexReader.getInstance().search(
				new QueryContext(acc,"dfl:0 flower"));
		assertEquals(0, res3.teasers.length);
		
		for ( int i=0; i<10; i++) {
			IndexWriter.getInstance().delete(ANONYMOUS, 
				docs.get(i).key,isMultiClient);
		}
	}
	
	public void testMetaFetchLimit() throws Exception  {
		String id = "ID025";
		
		ArrayList<HDocument> docs = new ArrayList<HDocument>();
		for ( int i=0; i<10; i++) {
			HDocument doc = new HDocument(ANONYMOUS);
			doc.key = i + " - " + id ;
			doc.title = "Flower " + i;
			docs.add(doc);
		}
		IndexWriter.getInstance().insertBatch(docs, acc, isMultiClient);

		QueryResult res1 = IndexReader.getInstance().search(
				new QueryContext(acc,"mfl:3 flower"));
		assertEquals(3, res1.teasers.length);

		QueryResult res2 = IndexReader.getInstance().search(
				new QueryContext(acc,"mfl:1 flower"));
		assertEquals(1, res2.teasers.length);
		
		QueryResult res3 = IndexReader.getInstance().search(
				new QueryContext(acc,"mfl:0 flower"));
		assertEquals(0, res3.teasers.length);
		
		for ( int i=0; i<10; i++) {
			IndexWriter.getInstance().delete(ANONYMOUS, 
				docs.get(i).key,isMultiClient);
		}
	}
	
	public void testTeaserForCacheText() throws Exception  {
		String id = "ID026";
		
		HDocument doc = new HDocument(ANONYMOUS);
		doc.key = id;
		doc.cacheText = "The default DateFormat instances returned by the static methods in the DateFormat class may be sufficient for many purposes , but clearly do not cover all possible valid or useful formats for dates. For example, notice that in Figure 2, none of the DateFormat-generated strings (numbers 2 - 9) match the format of the output of the Date class’s toString() method. This means that you cannot use the default DateFormat instances to parse the output of toString(), something that might be useful for things like parsing log data. The SimpleDateFormat lets you build custom formats. Dates are constructed with a string that specifies a pattern for the dates to be formatted and/or parsed. From the SimpleDateFormat JavaDocs, the characters in Figure 7 can be used in date formats. Where appropriate, 4 or more of the character will be interpreted to mean that the long format of the element should be used, while fewer than 4 mean that a short format should be used.";
		IndexWriter.getInstance().insert(doc, acc, isMultiClient);
		
		QueryResult middleCut = IndexReader.getInstance().search(
				new QueryContext(acc,"tsl:30 JavaDocs"));
		assertEquals(1, middleCut.teasers.length);
		
		DocTeaserWeight dtw = (DocTeaserWeight) middleCut.teasers[0]; 
		assertNotNull(dtw.cacheText);
		System.out.println(dtw.cacheText);
		assertEquals(" JavaDocs, the characters in", dtw.cacheText);
		System.out.println("JavaDocs://" + dtw.cacheText + "/" + dtw.cacheText.length());
		assertTrue( 50 >= dtw.cacheText.length());

		QueryResult frontCut = IndexReader.getInstance().search(
				new QueryContext(acc,"tsl:48 class"));
		assertEquals(1, frontCut.teasers.length);
		dtw = (DocTeaserWeight) frontCut.teasers[0]; 
		assertEquals(dtw.cacheText, " in the DateFormat class may be sufficient for");
		assertTrue( 68 >= dtw.cacheText.length());
		
		QueryResult endCut = IndexReader.getInstance().search(
				new QueryContext(acc,"tsl:36 parsing"));
		assertEquals(1, endCut.teasers.length);
		dtw = (DocTeaserWeight) endCut.teasers[0];
		System.out.println("parsing://" + dtw.cacheText);
		assertEquals(dtw.cacheText, " for things like parsing log data.");

		IndexWriter.getInstance().delete(ANONYMOUS, doc.key,isMultiClient);
	}
	
	public void testTeaserWithFields() throws Exception  {
		String id = "ID026";
		String text = "Adabra Cadabra";
		
		HDocument doc = new HDocument(ANONYMOUS);
		doc.key = id;
		doc.fields = new ArrayList<Field>();
		doc.fields.add(new SField("subject", text) );
		doc.cacheText = text;
		IndexWriter.getInstance().insert(doc, acc, isMultiClient);
		
		DocTeaserWeight dtw = null;
		QueryResult middleCut = IndexReader.getInstance().search(
				new QueryContext(acc,"tsl:30 adabra"));
		System.out.println(middleCut.toString());
		assertEquals(1, middleCut.teasers.length);
		
		dtw = (DocTeaserWeight) middleCut.teasers[0]; 
		assertNotNull(dtw.cacheText);
		assertEquals("Adabra", dtw.cacheText);


		IndexWriter.getInstance().delete(ANONYMOUS, doc.key,isMultiClient);
	}	
	
	public void testTeaserWithPreview() throws Exception  {
		String id = "TATA707";
		String text = "As others have said, enums are reference types - they're just compiler syntactic sugar for specific classes. The Adabra Cadabra JVM has no knowledge of them. That means the default value for the type is null. This doesn't just affect arrays, of course - it means the initial value of any field whose type is an enum is also null.";
		
		HDocument doc = new HDocument(ANONYMOUS);
		doc.key = id;
		doc.fields = new ArrayList<Field>();
		doc.fields.add(new SField("subject", text) );
		doc.cacheText = text;
		doc.preview = text;
		IndexWriter.getInstance().insert(doc, acc, isMultiClient);
		
		DocTeaserWeight dtw = null;
		String keyword = "initial";
		QueryResult middleCut = IndexReader.getInstance().search(
				new QueryContext(acc,"tsl:48 " + keyword));
		System.out.println(middleCut.toString());
		assertEquals(1, middleCut.teasers.length);
		
		dtw = (DocTeaserWeight) middleCut.teasers[0]; 
		if ( dtw.cacheText.indexOf(keyword) == -1) {
			System.out.println("cacheText:" + dtw.cacheText);
		}
		assertTrue(dtw.cacheText.indexOf(keyword) >= 0);
		assertEquals(dtw.preview, StringEscapeUtils.escapeXml(text));

		IndexWriter.getInstance().delete(ANONYMOUS, doc.key,isMultiClient);
	}		
	
	public void testAccessAllow() throws Exception  {
		String id = "ID027";
		
		HDocument doc = new HDocument(ANONYMOUS);
		doc.key = id;
		doc.title = "Welcome to IIT Library";
		AccessDefn viewPerm = new AccessDefn();
		viewPerm.ous = new String[] {"bizosys"};
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc, acc, isMultiClient);
		
		WhoAmI whoami = new WhoAmI();
		whoami.ou = "bizosys";
		whoami.uid = "n-4501";
		
		QueryContext ctx = new QueryContext(acc,"IIT");
		ctx.user = whoami;
		QueryResult res = IndexReader.getInstance().search(ctx); 
		assertEquals(1, res.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc.key,isMultiClient);
	}

	public void testAccessDeny() throws Exception {
		String id = "ID028";
		
		HDocument doc = new HDocument(ANONYMOUS);
		doc.key = id;
		doc.title = "Welcome to IIT Library";
		AccessDefn viewPerm = new AccessDefn();
		viewPerm.ous = new String[] {"bizosys"};
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc, acc, isMultiClient);
		
		WhoAmI whoami = new WhoAmI();
		whoami.ou = "infosys";
		whoami.uid = "n-4501";
		
		QueryContext ctx = new QueryContext(acc,"IIT");
		ctx.user = whoami;
		QueryResult res = IndexReader.getInstance().search(ctx); 
		assertEquals(0, res.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc.key,isMultiClient);
	}

	public void testGuest() throws Exception {
		String id = "ID029";
		
		HDocument doc = new HDocument(ANONYMOUS);
		doc.key = id;
		doc.title = "Register for private blogging at VOX.";
		AccessDefn viewPerm = new AccessDefn();
		viewPerm.ous = new String[] {"bizosys"};
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc, acc, isMultiClient);
		
		QueryContext ctx = new QueryContext(acc,"VOX");
		QueryResult res = IndexReader.getInstance().search(ctx); 
		assertEquals(0, res.teasers.length);

		IndexWriter.getInstance().delete(ANONYMOUS, doc.key,isMultiClient);		
	}

	public void testAccessAnonymous() throws Exception  {
		String id = "ID030";

		HDocument doc = new HDocument(ANONYMOUS);
		doc.key = id;
		doc.title = "Manmohan Singh is prime minister of India";
		AccessDefn viewPerm = new AccessDefn();
		viewPerm.uids = new String[] {Access.ANY};
		doc.viewPermission = viewPerm; 
		IndexWriter.getInstance().insert(doc, acc, isMultiClient);
		
		IndexWriter.getInstance().delete(ANONYMOUS, doc.key,isMultiClient);		

	}
	
	public void testSocialText() throws Exception  {
		HDocument doc1 = new HDocument(ANONYMOUS);
		doc1.key = "intitle";
		doc1.title = "Manmohan Singh is prime minister of India";
		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
		
		HDocument doc2 = new HDocument(ANONYMOUS);
		doc2.key = "intitle 2";
		doc2.title = "Manmohan Singh is prime minister of India. Manmohan is a great writer.";
		IndexWriter.getInstance().insert(doc2, acc, isMultiClient);
		
		HDocument doc3 = new HDocument(ANONYMOUS);
		doc3.key = "inbody";
		doc3.title = "Prime ministers of India.";
		doc3.cacheText = "Currently Manmohan is our prime minister";
		IndexWriter.getInstance().insert(doc3, acc, isMultiClient);
		
		HDocument doc4 = new HDocument(ANONYMOUS);
		doc4.key = "intitle social";
		doc4.title = "Prime ministers of India, Manmohan Singh";
		doc4.cacheText = "He is married and internalize his angers";
		
		IndexWriter.getInstance().insert(doc4, acc, isMultiClient);
		
		if ( 1 == 1) {
			QueryContext ctx2 = new QueryContext(acc,"Manmohan");
			QueryResult res2 = IndexReader.getInstance().search(ctx2);
			
			System.out.println(res2.toString());
			
			assertEquals(4, res2.teasers.length);
			assertEquals("intitle 2", ((DocTeaserWeight)res2.teasers[0]).id);
			assertEquals("inbody", ((DocTeaserWeight)res2.teasers[3]).id);
		}
		
		HDocument doc5 = doc4;
		doc5.socialText = new ArrayList<String>();
		doc5.socialText.add("Manmohan");
		IndexWriter.getInstance().insert(doc5, acc, isMultiClient);
		
		if ( 1 == 1) {
			QueryContext ctx = new QueryContext(acc,"Manmohan");
			QueryResult res = IndexReader.getInstance().search(ctx);
			assertEquals(4, res.teasers.length);
			assertEquals("intitle social", ((DocTeaserWeight)res.teasers[0]).id);
			assertEquals("intitle 2", ((DocTeaserWeight)res.teasers[1]).id);
			assertEquals("intitle", ((DocTeaserWeight)res.teasers[2]).id);
			assertEquals("inbody", ((DocTeaserWeight)res.teasers[3]).id);
		}

		List<String> docIds = new ArrayList<String>();
		docIds.add(doc1.key);
		docIds.add(doc2.key);
		docIds.add(doc3.key);
		docIds.add(doc4.key);
		IndexWriter.getInstance().delete(ANONYMOUS, docIds);
	}
	
	public void testNlp() throws Exception  {
        final String [][] data = new String [] []
        {
            {
                "http://en.wikipedia.org/wiki/Data_mining",
                "Data mining - Wikipedia, the free encyclopedia",
                "Article about knowledge-discovery in databases (KDD), the practice of automatically searching large stores of data for patterns."
            },

            {
                "http://www.ccsu.edu/datamining/resources.html",
                "CCSU - Data Mining",
                "A collection of Data Mining links edited by the Central Connecticut State University ... Graduate Certificate Program. Data Mining Resources. Resources. Groups ..."
            },

            {
                "http://www.kdnuggets.com/",
                "KDnuggets: Data Mining, Web Mining, and Knowledge Discovery",
                "Newsletter on the data mining and knowledge industries, offering information on data mining, knowledge discovery, text mining, and web mining software, courses, jobs, publications, and meetings."
            },

            {
                "http://en.wikipedia.org/wiki/Data-mining",
                "Data mining - Wikipedia, the free encyclopedia",
                "Data mining is considered a subfield within the Computer Science field of knowledge discovery. ... claim to perform \"data mining\" by automating the creation ..."
            },

            {
                "http://www.anderson.ucla.edu/faculty/jason.frand/teacher/technologies/palace/datamining.htm",
                "Data Mining: What is Data Mining?",
                "Outlines what knowledge discovery, the process of analyzing data from different perspectives and summarizing it into useful information, can do and how it works."
            },
        };

        for (String [] row : data)
        {
    		HDocument doc1 = new HDocument(ANONYMOUS);
    		doc1.key = row[0];
    		doc1.title = row[1];
    		doc1.cacheText = row[2];
    		IndexWriter.getInstance().insert(doc1, acc, isMultiClient);
        }
        
		QueryContext ctx = new QueryContext(acc,"data cluster:nlp");
		QueryResult res = IndexReader.getInstance().search(ctx);
		System.out.println(res.toString());
		
	}
}
