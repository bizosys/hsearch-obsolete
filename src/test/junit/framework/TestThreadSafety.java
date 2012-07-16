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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.Test;

public class TestThreadSafety extends TestRandomValue implements Runnable {

	public List<String> results = null;  
	CountDownLatch startSignal = null;
	CountDownLatch doneSignal = null;
	TestCase testCase = null;

	public TestThreadSafety(TestCase testCase, List<String> results,
	CountDownLatch startSignal, CountDownLatch doneSignal) {
		this.testCase = testCase;
		this.results = results;
		this.startSignal = startSignal;
		this.doneSignal = doneSignal;
	}
	
	public void run() {
		System.out.println("Running thread :" + Thread.currentThread().getName());
		super.verbose = false;
		super.displayText = false;

		try {
			startSignal.await();
			testCase.setUp();
			super.run(this.testCase);
			testCase.tearDown();
			this.results.add(toString());
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			doneSignal.countDown();
		}
	}
		
	@Override
	protected void invoke(Method runMethod, Test testCase, Object[] values) throws Exception {
		long startTime = System.currentTimeMillis();
		runMethod.invoke(testCase, values);
		long endTime = System.currentTimeMillis();
		
		long delta = endTime - startTime;
		if (delta > 3000) throw new SlowFunctionException(
			runMethod.getName() + " > " + delta + " ms");
		this.totalSucess++;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		for (String key : responseTimes.keySet()) {
			sb.append("\n").append(key).append("=" ).append(responseTimes.get(key)).append(" ms");
		}
		return super.toString() + sb.toString();
	}	
	
	public static List<TestThreadSafety> runInParallel(
		TestCase testCase, int threads) throws Exception {
				
		CountDownLatch startSignal = new CountDownLatch(1);
		CountDownLatch doneSignal = new CountDownLatch(threads);
		
		List<TestThreadSafety> tests = new ArrayList<TestThreadSafety>(threads);
		for ( int i=0; i< threads; i++ ) {
			List<String> results = new ArrayList<String>();
			TestThreadSafety  test = new TestThreadSafety(
				testCase.getClass().newInstance(), results, startSignal, doneSignal);
			tests.add(test);
			new Thread(test).start();
		}
		
		startSignal.countDown();
		doneSignal.await();

		return tests;
	}


	public static void main(String[] args) throws Exception {
        TestCase testCase = new DryRunTest();
        List<TestThreadSafety> tests = TestThreadSafety.runInParallel(testCase, 10);
        for (TestThreadSafety aTest : tests) {
        	System.out.println(aTest.toString());
		}
        
    }
	
}
