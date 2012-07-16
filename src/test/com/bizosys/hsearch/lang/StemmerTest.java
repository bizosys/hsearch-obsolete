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
package com.bizosys.hsearch.lang;

import junit.framework.TestCase;
import junit.framework.TestFerrari;


public class StemmerTest extends TestCase {

	public static void main(String[] args) throws Exception {
		StemmerTest t = new StemmerTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize() {
		Stemmer ls = Stemmer.getInstance();
		String[] words = new String[]{"onsequential", "corporating", "corporatist", "abatable",
				"abate","abatement", "abatic","abating", "abatis", "abattis",
				"abattoir","abaxial", "abaxially", "abaya" };
		
		for (String word : words) {
			System.out.println( word + "=" + ls.stem(word));
		}
	}
}
