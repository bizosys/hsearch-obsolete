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
package com.bizosys.hsearch.dictionary;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Hashtable;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.scheduler.SchedulerService;
import com.bizosys.oneline.util.StringUtils;

public class DictionaryManagerTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DictionaryManagerTest t = new DictionaryManagerTest();
		TestFerrari.testAll(t);
		//t.testSpellCorrection();
	}
	
	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		SchedulerService.getInstance().init(conf, null);
		new DictionaryManager();
		DictionaryManager.getInstance().init(new Configuration(), null);
		DictionaryManager.getInstance().purge();
	}
	
	@Override
	protected void tearDown() throws Exception {
		SchedulerService.getInstance().stop();
	}
	
	public String ANONYMOUS = "anonymous";
	
	public void testRandomAddEntry(String keyword) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry e1 = new DictEntry(keyword,"random", 1, null,null);
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>();
		entries.put(e1.word, e1);
		s.add(ANONYMOUS, entries);
		s.delete(ANONYMOUS, keyword);
	}
	
	public void testAddEntries(String keyword1, String keyword2, String keyword3, 
			String keyword4, String keyword5, String keyword6, String keyword7,  
			String keyword8, String keyword9, String keyword10) throws Exception {

		DictionaryManager s = DictionaryManager.getInstance();
		String[] keywords = new String[] {
				keyword1, keyword2, keyword3, keyword4, keyword5,
				keyword6, keyword7, keyword8, keyword9, keyword10
		};
		Hashtable<String, DictEntry> inMap = new Hashtable<String, DictEntry>();
		for (String keyword : keywords) {
			DictEntry in = new DictEntry(keyword);
			inMap.put(keyword, in);
		}
		s.add(ANONYMOUS, inMap);
		
		for (String keyword : keywords) {
			DictEntry out = s.get(ANONYMOUS, keyword);
			assertNotNull(out);
			assertEquals(keyword, out.word);
			s.delete(ANONYMOUS, keyword);
		}
	}
	

	public void testGetEntry(String keyword, String type, Integer freq, String related, String detail) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry in = new DictEntry(keyword,type, freq, related,detail);
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>();
		entries.put(in.word, in);
		s.add(ANONYMOUS, entries);
		
		DictEntry entry = s.get(ANONYMOUS, keyword);
		assertNotNull(entry);
		assertEquals(keyword, entry.word);
		assertEquals(type.trim().toLowerCase(), entry.type);
		assertEquals(freq.intValue(), entry.frequency);
		assertEquals(related, entry.related);
		assertEquals(detail, entry.detail);
		
		s.delete(ANONYMOUS, keyword);
	}
	
	public void testGetEmpty() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry entry = s.get(ANONYMOUS, "");
		assertNull(entry);
	}
	
	public void testNonExisting() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		DictEntry entry = s.get(ANONYMOUS, "__aSDKJ234KSAKL1adsa");
		assertNull(entry);
	}

	public void testSpellCorrection() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		s.add(ANONYMOUS, new DictEntry("Abinasha", "Fuzzy",1));
		s.add(ANONYMOUS, new DictEntry("Abhinasha", "Fuzzy",1));
		s.add(ANONYMOUS, new DictEntry("Avinash", "Fuzzy",1));
		
		s.add("bizosys", new DictEntry("Abinasha", "Fuzzy",1));
		s.refresh(ANONYMOUS);
		
		List<String> fuzzyWords = s.getSpelled(ANONYMOUS, "abinash");		
		assertNotNull(fuzzyWords);
		if ( 1 != fuzzyWords.size()) System.out.println(StringUtils.listToString(fuzzyWords, '\n'));
		assertEquals(2 , fuzzyWords.size());
		assertEquals("Abhinasha", fuzzyWords.get(0));
		assertEquals("Abinasha", fuzzyWords.get(1));
		
		s.delete(ANONYMOUS, "Abinasha");
		s.delete(ANONYMOUS, "Abhinasha");
		s.delete(ANONYMOUS, "Avinash");
		s.delete("bizosys", "Abinasha");
		
	}

	public void testWildcard() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		s.add(ANONYMOUS, new DictEntry("Sunil", "" , 1));
		s.refresh(ANONYMOUS);
		List<String> regexWords = s.getWildCard(ANONYMOUS, ".unil");		
		assertNotNull(regexWords);
		assertEquals(1, regexWords.size());
		assertEquals("Sunil", regexWords.get(0));
		
		s.delete(ANONYMOUS, "Sunil");
	}
	
	public void testResolveTypes(String keyword, String type1, String type2 ) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		s.add(ANONYMOUS, new DictEntry(keyword,type1,1,null,null));
		s.add(ANONYMOUS, new DictEntry(keyword,type2,1,null,null));
		
		DictEntry entry = s.get(ANONYMOUS, keyword);
		assertNotNull(entry);
		List<String> types = entry.getTypes();
		assertNotNull(types);
		if ( 2 != types.size()) System.out.println(
			"INSTR: " + "Keyword:[" + keyword + "] , Types =" + 
			StringUtils.listToString(types, '\n') );
		assertEquals(2, types.size() );
		type1 = type1.toLowerCase().trim();
		type2 = type2.toLowerCase().trim();
		System.out.println(type1 + "==" + types.get(0) + "  or " + type1 + "==" + types.get(1));
		System.out.println(type2 + "==" + types.get(0) + "  or " + type2 + "==" + types.get(1));
		assertTrue(type1.equals(types.get(0)) || type1.equals(types.get(1)) );
		assertTrue(type2.equals(types.get(0)) || type2.equals(types.get(1))) ;
		
		s.delete(ANONYMOUS, keyword);
	}
	
	public void testTermFrequency(String keyword) throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		s.add(ANONYMOUS, new DictEntry(keyword,"freq",1));
		s.add(ANONYMOUS, new DictEntry(keyword,"freq",1));
		
		DictEntry entry = s.get(ANONYMOUS, keyword);
		assertNotNull(entry);
		assertEquals(2, entry.frequency);
		s.delete(ANONYMOUS, keyword);
	}
	
	/**
	public void testWordPaging() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		List<String> allWords = s.getKeywords(ANONYMOUS);
		StringBuilder sb = new StringBuilder();
		sb.append("Page 1: " );
		sb.append(StringUtils.listToString(allWords, '\t'));
		System.out.println(sb.toString());
		sb.delete(0, sb.capacity());
		int page = 2;
		String lastWord = null;
		while ( allWords.size() > 1000) {
			lastWord = allWords.get(allWords.size() - 1);
			allWords.clear();
			allWords = s.getKeywords(lastWord);
			sb.append("Page " ).append(page++).append(" : ");
			sb.append(StringUtils.listToString(allWords, '\t'));
			System.out.println(sb.toString());
			sb.delete(0, sb.capacity());
		}
	}
	*/
	
	public void testWordListing() throws Exception {
		DictionaryManager s = DictionaryManager.getInstance();
		Hashtable<String, DictEntry> entries = new Hashtable<String, DictEntry>();
		for ( int i=0; i< 1000; i++) {
			DictEntry e1 = new DictEntry("word-" + i,"unknown", 1, null,null);
			entries.put(e1.word, e1);
		}
		s.add(ANONYMOUS, entries);

		
		Writer writer = new StringWriter();
		s.getKeywords(ANONYMOUS, writer);
		System.out.println("Listing :" + writer.toString());
	}	

}
