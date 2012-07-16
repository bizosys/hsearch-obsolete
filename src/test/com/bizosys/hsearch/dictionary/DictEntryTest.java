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

import junit.framework.TestCase;
import junit.framework.TestFerrari;

public class DictEntryTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DictEntryTest t = new DictEntryTest();
        TestFerrari.testRandom(t);
	}
	
	public void goodValues(String keyword, String type, Integer freq,
			String related, String detail) throws Exception {
		DictEntry e1 = new DictEntry(keyword,type,freq, related,detail);
		DictEntry e2 = new DictEntry(e1.toBytes()) ;
		assertEquals(detail, e2.detail);
		assertEquals(freq.intValue(), e2.frequency);
		assertEquals(related, e2.related);
		assertEquals(type.trim().toLowerCase(), e2.type);
		assertEquals(keyword, e2.word);
	}

	public void nullValues(String keyword) throws Exception {
		DictEntry e1 = new DictEntry(keyword);
		DictEntry e2 = new DictEntry(e1.toBytes()) ;
		assertEquals(e1.detail, e2.detail);
		assertEquals(e1.frequency, e2.frequency);
		assertEquals(e1.related, e2.related);
		assertEquals(e1.type, e2.type);
		assertEquals(e1.word, e2.word);
	}
}
