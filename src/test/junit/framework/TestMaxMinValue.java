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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Test;

public class TestMaxMinValue extends TestRandomValue{

	@Override
	protected void invoke(Method runMethod, Test testCase, Object[] values) throws Exception {
		runMethod.invoke(testCase, values);
	}
	
	@Override
	protected void runMethod(Method runMethod, Test testCase) {
		if (! Modifier.isPublic(runMethod.getModifiers())) {
			return;
		}
		String methodName = runMethod.getName();
		if ( "main".equals(methodName) ) return;
		
		if ( verbose )
			System.out.println("\n... Checking Random Value test method >>" +  methodName + "<<" );
		Class[] params = runMethod.getParameterTypes();
		int iteration = getIterations();
		List<Object[]> values = new ArrayList<Object[]>(iteration);
		
		for (int i=0; i< iteration; i++) {
			values.add(new Object[params.length]); 
		}
		
		int counter = 0;

		boolean neumeric = false;
		for (Class param : params) {
			if ( String.class == param) {
			} else if (Double.class == param) { 
				values.get(0)[counter] = Double.MIN_VALUE;
				values.get(1)[counter] = Double.MAX_VALUE;
				values.get(2)[counter] = new Double(0);
				values.get(3)[counter] = new Double(1);
				values.get(4)[counter] = new Double(-1);
				neumeric = true;
			} else if (Long.class == param) { 
				values.get(0)[counter] = Long.MIN_VALUE;
				values.get(1)[counter] = Long.MAX_VALUE;
				values.get(2)[counter] = new Long(0);
				values.get(3)[counter] = new Long(1);
				values.get(4)[counter] = new Long(-1);
				neumeric = true;
			} else if (Integer.class == param) { 
				values.get(0)[counter] = Integer.MIN_VALUE;
				values.get(1)[counter] = Integer.MAX_VALUE;
				values.get(2)[counter] = new Integer(0);
				values.get(3)[counter] = new Integer(1);
				values.get(4)[counter] = new Integer(-1);
				neumeric = true;
			} else if (Float.class == param) { 
				values.get(0)[counter] = Float.MIN_VALUE;
				values.get(1)[counter] = Float.MAX_VALUE;
				values.get(2)[counter] = (float) 0;
				values.get(3)[counter] = (float) 1;
				values.get(4)[counter] = (float) -1;
				neumeric = true;
			} else if (Short.class == param) { 
				values.get(0)[counter] = Short.MIN_VALUE;
				values.get(1)[counter] = Short.MAX_VALUE;
				values.get(2)[counter] = new Short((short)0);
				values.get(3)[counter] = new Short((short)1);
				values.get(4)[counter] = new Short((short)-1);
				neumeric = true;
			} else if (Byte.class == param) { 
				values.get(0)[counter] = Byte.MIN_VALUE;
				values.get(1)[counter] = Byte.MAX_VALUE;
				values.get(2)[counter] = (byte) 0;
				values.get(3)[counter] = (byte) 1;
				values.get(4)[counter] = (byte) -1;
				neumeric = true;
			} 
			counter++;
		}
		if ( !neumeric) return;
		
		try {
			totalRun++;
			for (Object[] objects : values) {
				invoke(runMethod, testCase, objects);
			}
			this.totalSucess++;
			
		} catch (Exception ex) {
			StringBuilder sb = new StringBuilder();
			sb.append(testCase.getClass().getName() + ":" + runMethod.getName()); 
			for (Object[] objects : values) {
				if ( null == objects) sb.append(" [Params : N/A]");
				else {
					for (Object obj : objects ) {
						if ( null == obj) sb.append(" [Param : Null] " );
						else sb.append(" [Param : " + obj.toString() + "]");
					}
				}
			}
			
			System.out.println("########## Failed :" + sb.toString());
			ex.printStackTrace(System.err);
			failedFunctions.add(runMethod.getName());
		}
	}
	
    public static void main(String[] args) throws Exception {
        Test testCase = new DryRunTest();
        TestMaxMinValue tester = new TestMaxMinValue(); 
        tester.run(testCase);
        System.out.println(tester.toString());
    }
	
}
