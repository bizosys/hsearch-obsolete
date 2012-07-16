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

import java.util.Date;

import junit.framework.TestCase;
import junit.framework.TestFerrari;


public class DocMetaTest extends TestCase {

	public static void main(String[] args) throws Exception {
		DocMetaTest t = new DocMetaTest();
        TestFerrari.testAll(t);
	}
	
	public void testSerialize(String type, String state, 
		String orgunit, String geohouse, Long created, Long modified, 
		Long validTill, Boolean secuity, Boolean sentiment) throws Exception {

		DocMeta meta = new DocMeta();
		meta.docType = type;
		meta.state = state;
		meta.team = orgunit;
		meta.geoHouse= geohouse;
		meta.createdOn = new Date(created);
		meta.modifiedOn = new Date(modified);
		meta.validTill = new Date(validTill);
		meta.securityHigh = secuity;
		meta.sentimentPositive = sentiment;
		
		byte[] bytes = meta.toBytes();
		DocMeta deserialized = new DocMeta(bytes);
		
		assertEquals(type, deserialized.docType);
		assertEquals(state, deserialized.state);
		assertEquals(orgunit, deserialized.team);
		assertEquals(geohouse, deserialized.geoHouse);

		assertEquals(created.longValue(), deserialized.createdOn.getTime());
		assertEquals(modified.longValue(), deserialized.modifiedOn.getTime());
		assertEquals(validTill.longValue(), deserialized.validTill.getTime());
		
		assertEquals(secuity.booleanValue(), deserialized.securityHigh);
		assertEquals(sentiment.booleanValue(), deserialized.sentimentPositive);
		
	}
}
