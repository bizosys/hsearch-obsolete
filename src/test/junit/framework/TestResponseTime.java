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

import junit.framework.Test;

public class TestResponseTime extends TestRandomValue{
	
	protected int maxTolerableInMillis = 100;
	public TestResponseTime(int maxTolerableInMillis) {
		super.iteration = 10;
		this.maxTolerableInMillis = maxTolerableInMillis;
	}
	
	@Override
	public void run(Test testCase) throws Exception {
		super.verbose = false;
		super.run(testCase);
	}
	
	@Override
	protected void runMethod(Method runMethod, Test testCase) {
		System.out.println("\n... Checking Response Time test method >>" +  runMethod.getName() + "<<" );
		super.runMethod(runMethod, testCase);
	}
	
	
	@Override
	protected void invoke(Method runMethod, Test testCase, Object[] values) throws Exception {
		long startTime = System.currentTimeMillis();
		runMethod.invoke(testCase, values);
		long endTime = System.currentTimeMillis();
		long delta = endTime - startTime;
		String runMethodName = runMethod.getName();
		if ( responseTimes.containsKey(runMethodName) ) {
			float avg =  (responseTimes.get(runMethodName) + delta) / 2;
			responseTimes.put(runMethodName, avg);
		} else {
			responseTimes.put(runMethodName, new Long(delta).floatValue());
		}
		if ( delta >= maxTolerableInMillis ) {
			failedFunctions.add(runMethod.getName());
		}
		
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(100);
		for (String key : responseTimes.keySet()) {
			sb.append("  , ").append(key).append("=" ).append(responseTimes.get(key)).append(" ms");
		}
		return super.toString() + sb.toString();
	}
	
    public static void main(String[] args) throws Exception {
        Test testCase = new DryRunTest();
        TestResponseTime tester = new TestResponseTime(5); 
        tester.run(testCase);
        System.out.println(tester.toString());
    }
}