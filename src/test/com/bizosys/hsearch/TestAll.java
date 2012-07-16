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
package com.bizosys.hsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestFerrari;
import junit.framework.TestRandomValue;

import com.bizosys.hsearch.common.AccessControlTest;
import com.bizosys.hsearch.dictionary.DictEntryTest;
import com.bizosys.hsearch.dictionary.DictionaryManagerTest;
import com.bizosys.hsearch.dictionary.DistanceTest;
import com.bizosys.hsearch.filter.AMFilterCommonTest;
import com.bizosys.hsearch.filter.StorableListTest;
import com.bizosys.hsearch.index.BuildTeaserHighlighterTest;
import com.bizosys.hsearch.index.DocMetaTest;
import com.bizosys.hsearch.index.DocTeaserTest;
import com.bizosys.hsearch.index.DocumentTypeTest;
import com.bizosys.hsearch.index.IndexReaderTest;
import com.bizosys.hsearch.index.IndexWriterTest;
import com.bizosys.hsearch.index.InvertedIndexTest;
import com.bizosys.hsearch.index.TermListTest;
import com.bizosys.hsearch.index.TermTest;
import com.bizosys.hsearch.index.TermTypeTest;
import com.bizosys.hsearch.lang.StemmerTest;
import com.bizosys.hsearch.outpipe.HQueryParserTest;
import com.bizosys.hsearch.outpipe.QuerySequencingTest;
import com.bizosys.hsearch.util.UrlShortnerTest;

public class TestAll extends TestCase {

	public static void main(String[] args) throws Exception {

		TestCase[] testCases = new TestCase[] {
				new AccessControlTest(), new StorableListTest(), new DistanceTest(),
				new DictEntryTest(), new DictionaryManagerTest(), new AMFilterCommonTest(),
				new BuildTeaserHighlighterTest(), new BuildTeaserHighlighterTest(),
				new DocMetaTest(), new DocTeaserTest(), new DocumentTypeTest(),
				new HQueryParserTest(), new QuerySequencingTest(),new UrlShortnerTest(),
				new IndexReaderTest(),new IndexWriterTest(),new InvertedIndexTest(),
				new TermListTest(),new TermTest(),new TermTypeTest(),new StemmerTest(),
				new TermTypeTest()
		};
		
		TestAll.run(testCases);
	}
	
	public static void run(TestCase[] testCases) throws Exception {
		Map<String, String> failed = new HashMap<String, String>();;
		
		/**  "random" , "special"  , "i18n"  , "response" , "maxmin" , "memory" ,  "null"  ,  "empty"  ,   "parallel"*/
		
		String[] testmodes = new String[] {
			"random" , "special", "response" , "maxmin" , "memory" ,  "null"  ,  "empty" ,   "parallel"};
		
		int totalRun = 0;
		int totalSucess = 0;
		int totalFailure = 0;

		for (String mode : testmodes) {
			for (TestCase case1 : testCases) {
				String testClazz = case1.getClass().getName() + "/";
				try {
					List<TestRandomValue> testResults = new ArrayList<TestRandomValue>();
					if ( "random".equals(mode) ) testResults.add(TestFerrari.testRandom(case1));
					else if ( "special".equals(mode) ) testResults.add(TestFerrari.testSpecial(case1));
					else if ( "i18n".equals(mode) ) testResults.add(TestFerrari.testI18N(case1));
					else if ( "response".equals(mode) ) testResults.add(TestFerrari.testResponse(case1));
					else if ( "maxmin".equals(mode) ) testResults.add(TestFerrari.testMaxMin(case1));
					else if ( "memory".equals(mode) ) testResults.add(TestFerrari.testMemory(case1));
					else if ( "null".equals(mode) ) testResults.add(TestFerrari.testNull(case1));
					else if ( "empty".equals(mode) ) testResults.add(TestFerrari.testEmpty(case1));
					
					else if ( "parallel".equals(mode) ) testResults.addAll(TestFerrari.testParallel(case1));
					else throw new Exception("Unknown mode :" + mode);
					
					for (TestRandomValue testResult : testResults) {
						totalRun = totalRun + testResult.getRuns();
						totalSucess = totalSucess + testResult.getSucesses();
						totalFailure = totalFailure + testResult.getFailures();
						failed.put(testClazz + mode, testResult.getFailedFunctions());
					}
					testResults.clear();
					
				} catch (Exception ex) {
					ex.printStackTrace(System.err);
				}
			}
		}
		
		System.out.println("############# Run Report ################");
		System.out.println("Total Run:" + totalRun);
		System.out.println("Total Sucess:" + totalSucess);
		System.out.println("Total Failures:" + totalFailure);
		
		for (String testC : failed.keySet()) {
			System.out.println("Failed : " + testC + " = " + failed.get(testC).toString());
		}
	}
	

}
