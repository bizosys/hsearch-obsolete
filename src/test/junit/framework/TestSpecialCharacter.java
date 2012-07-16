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
import java.util.Date;
import java.util.List;

import junit.framework.Test;

public class TestSpecialCharacter extends TestRandomValue {

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
		
		boolean isString = false;
		for (Class param : params) {
			
			if ( String.class == param) {
				isString = true;
				char[] specials = "~`!@#$%^&*()_-+={[}]|\\:;\"'<,>.?/".toCharArray(); 
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = "special" + specials[i];
			} else if (Double.class == param) { 
				List<Double> samples = DataRandomPrimitives.getDouble(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Long.class == param) { 
				List<Long> samples = DataRandomPrimitives.getLong(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Integer.class == param) { 
				List<Integer> samples = DataRandomPrimitives.getInteger(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Float.class == param) { 
				List<Float> samples = DataRandomPrimitives.getFloat(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Short.class == param) { 
				List<Short> samples = DataRandomPrimitives.getShort(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Byte.class == param) { 
				List<Byte> samples = DataRandomPrimitives.getByte(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Boolean.class == param) { 
				List<Boolean> samples = DataRandomPrimitives.getBoolean(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			} else if (Date.class == param) { 
				List<Date> samples = DataRandomPrimitives.getDates(iteration);
				for ( int i=0; i< iteration; i++) values.get(i)[counter] = samples.get(i);
			}

			counter++;
		}
		if ( ! isString ) return;  
		System.out.println("\n... Checking Special characters | test method >>" +  methodName + "<<" );

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
    	new TestSpecialCharacter().run(testCase);
    }
	
}
