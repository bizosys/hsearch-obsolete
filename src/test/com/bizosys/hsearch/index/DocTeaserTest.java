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
package com.bizosys.hsearch.index;

import junit.framework.TestCase;
import junit.framework.TestFerrari;


public class DocTeaserTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DocTeaserTest t = new DocTeaserTest();
        TestFerrari.testRandom(t);
	}
	
	public void testSerialize(String id, String url, 
		String title, String cacheText, String preview) throws Exception {

		DocTeaser teaser = new DocTeaser();
		teaser.id = id;
		teaser.url = url;
		teaser.title = title;
		teaser.cacheText= cacheText;
		teaser.preview = preview;
		
		byte[] bytes = teaser.toBytes();
		DocTeaser deserialized = new DocTeaser("123".getBytes(), bytes);
		
		assertEquals( id, new String(deserialized.id));
		assertEquals(url, new String(deserialized.url));
		assertEquals(title, new String(deserialized.title));
		assertEquals(cacheText, new String(deserialized.cacheText));
		assertEquals(preview, new String(deserialized.preview));
		
	}
	
}
