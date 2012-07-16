/*
* Copyright 2010 The Apache Software Foundation
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
package junit.framework;

import java.util.List;


public class TestFerrari {
	
	public static TestRandomValue testRandom(TestCase testCase) throws Exception {
		TestRandomValue testType = new TestRandomValue();
		return runTest(testCase, testType);
	}
	
	public static TestRandomValue testSpecial(TestCase testCase) throws Exception {
		TestSpecialCharacter testType = new TestSpecialCharacter();
		return runTest(testCase, testType);
	}	

	public static TestRandomValue testI18N(TestCase testCase) throws Exception {
		TestI18N testType = new TestI18N();
		return runTest(testCase, testType);
	}

	public static TestRandomValue testResponse(TestCase testCase) throws Exception {
		TestResponseTime testType = new TestResponseTime(3000);
		return runTest(testCase, testType);
	}

	public static TestRandomValue testMaxMin(TestCase testCase) throws Exception {
		TestMaxMinValue testType = new TestMaxMinValue();
		return runTest(testCase, testType);
	}

	// Looks for maximum 1MB usage.
	public static TestRandomValue testMemory(TestCase testCase) throws Exception {
		TestRandomValue testType = new TestMemoryUsage(1024);
		return runTest(testCase, testType);
	}

	public static TestRandomValue testNull(TestCase testCase) throws Exception {
		TestNullValue testType = new TestNullValue();
		return runTest(testCase, testType);
	}
	
	public static TestRandomValue testEmpty(TestCase testCase) throws Exception {
		TestZeroOrEmptyValue testType = new TestZeroOrEmptyValue();
		return runTest(testCase, testType);
	}

	public static List<TestThreadSafety> testParallel(TestCase testCase) throws Exception {
		List<TestThreadSafety> tests = TestThreadSafety.runInParallel(testCase, 3);
		for (TestThreadSafety test : tests) {
			System.out.println("--------------------");
	        System.out.println(test.toString());
		}
        return tests;
	}	
	
	public static void testAll(TestCase testCase) throws Exception {
		
        TestRandomValue randomTest = new TestRandomValue(); 
        randomTest.run(testCase);

        TestI18N i18NTest = new TestI18N(); 
        i18NTest.run(testCase);

        TestSpecialCharacter spacialCharTest = new TestSpecialCharacter(); 
        spacialCharTest.run(testCase);
        
        TestMaxMinValue maxMinTest = new TestMaxMinValue(); 
        maxMinTest.run(testCase);
        
        TestResponseTime responseTest = new TestResponseTime(100); 
        responseTest.run(testCase);

        TestMemoryUsage memoryTest = new TestMemoryUsage(1024); 
        memoryTest.run(testCase);

        System.out.println("--------------------");
        System.out.println(randomTest.toString());
        System.out.println(i18NTest.toString());
        System.out.println(spacialCharTest.toString());
        System.out.println(responseTest.toString());
        System.out.println(maxMinTest.toString());
        System.out.println(memoryTest.toString());
	}
	
	private static TestRandomValue runTest(TestCase testCase, TestRandomValue testType) throws Exception{
		testCase.setUp();
		testType.run(testCase);
		testCase.tearDown();
		
        System.out.println("--------------------");
        System.out.println(testType.toString());
        
        return testType;
	}
}
