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
package com.bizosys.hsearch.util;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestFerrari;


public class UrlShortnerTest extends TestCase {

	public static void main(String[] args) throws Exception {
		UrlShortnerTest t = new UrlShortnerTest();
        TestFerrari.testRandom(t);
	}
	
	public void testSerialize() throws Exception{
		UrlShortner mapper = UrlShortner.getInstance();
		Map<String, String> mappings = new HashMap<String, String>();
		mappings.put("http://www.bizosys.com/employee.xml/id=", "a1");
		mappings.put("http://www.bizosys.com/employee", "a2");
		mapper.persist(mappings);
		mapper.refresh();
		
		String equalPrifix = "http://www.bizosys.com/employee.xml/id=23";
		String encoded = mapper.encoding(equalPrifix);
		assertEquals("a1~23", encoded);
		assertEquals(equalPrifix, mapper.decoding(encoded));

		String questionPrifix = "http://www.bizosys.com/employee?23";
		encoded = mapper.encoding(questionPrifix);
		assertEquals("a2~?23", encoded);
		assertEquals(questionPrifix, mapper.decoding(encoded));

		String unknownUrl = "http://www.google.com/employee?23";
		encoded = mapper.encoding(unknownUrl);
		assertEquals(unknownUrl, encoded);
		assertEquals(unknownUrl, mapper.decoding(encoded));
	}
	
	public void testNull() throws Exception{
		UrlShortner mapper = UrlShortner.getInstance();
		Map<String, String> mappings = new HashMap<String, String>();
		mapper.persist(mappings);
		mapper.refresh();

		String unknownUrl = "http://www.bizosys.com/employee.xml/id=23";
		String encoded = mapper.encoding(unknownUrl);
		assertEquals(unknownUrl, encoded);
		assertEquals(unknownUrl, mapper.decoding(encoded));
	}	
}
