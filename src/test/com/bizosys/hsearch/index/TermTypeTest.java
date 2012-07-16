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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.TestAll;
import com.bizosys.hsearch.common.Account;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.dictionary.DictionaryManager;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;

public class TermTypeTest extends TestCase {

	public static void main(String[] args) throws Exception {
		TermTypeTest t = new TermTypeTest();
		String[] modes = new String[] { "all", "random", "method"};
		String mode = modes[2];
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testResponse(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testFillup();
			t.tearDown();
		}
	}

	boolean isMultiClient = true;
	AccountInfo acc = null;
	boolean concurrency = true;
	String ANONYMOUS = "anonymous";
	
	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		ServiceFactory.getInstance().init(conf, null);

		this.ANONYMOUS = Thread.currentThread().getName();
		this.acc = Account.getAccount(ANONYMOUS);
		if ( null == acc) {
			acc = new AccountInfo(ANONYMOUS);
			acc.name = ANONYMOUS;
			acc.maxbuckets = 1;
			Account.storeAccount(acc);
		}

		DictionaryManager.getInstance().purge();

	}

	@Override
	protected void tearDown() throws Exception {
		ServiceFactory.getInstance().stop();
	}
	
	public void testPersist(Boolean isMultiClient) throws Exception {
		TermType type = TermType.getInstance(isMultiClient);
		Map<String, Byte> tcodes = new HashMap<String, Byte>();
		tcodes.put("Employee", (byte) -128);
		tcodes.put("customers", (byte) -127);
		
		type.persist("ANONYMOUS", tcodes);
		assertTrue( (byte) -127 ==  type.getTypeCode("ANONYMOUS", "customers"));
	}

	public void testMultiTenant(String aCode, String bCode, String aTenant, String bTenant) throws Exception {
		TermType type = TermType.getInstance(isMultiClient);
		Map<String, Byte> tcodes1 = new HashMap<String, Byte>();
		tcodes1.put(aCode, (byte) -128);
		tcodes1.put(bCode, (byte) -127);
		type.persist(aTenant, tcodes1);
		
		Map<String, Byte> tcodes2 = new HashMap<String, Byte>();
		tcodes2.put(aCode, (byte) -128);
		tcodes2.put(bCode, (byte)34);
		type.persist(bTenant, tcodes2);

		assertTrue( (byte) -127 ==  type.getTypeCode(aTenant, bCode));
		assertTrue( (byte) 34 ==  type.getTypeCode(bTenant, bCode));
	}
	
	public void testAutoIncrement() throws Exception {
		TermType type = TermType.getInstance(isMultiClient);
		Map<String, Byte> tcodes = new HashMap<String, Byte>();
		
		Random random = new Random();
		int pos1 = random.nextInt(Byte.MAX_VALUE);
		int pos2 = random.nextInt(Byte.MAX_VALUE);
		
		tcodes.put("codex", (byte)pos1);
		tcodes.put("codey", (byte)pos2);
		for ( int i=Byte.MIN_VALUE; i<= Byte.MAX_VALUE - 2; i++) {
			type.autoInsert(tcodes, ("code" + i));
		}
		
		System.out.println(type.toString(tcodes));

		assertEquals((byte)tcodes.get("codex"), (byte)pos1);
		assertEquals((byte)tcodes.get("codey"), (byte)pos2);

		Set<String> uniqueBytes = new HashSet<String>();
		for (Byte val : tcodes.values()) {
			uniqueBytes.add(val.toString());
		}
		assertEquals(uniqueBytes.size(), 256);
	}
	
	public void testFillup() throws Exception {
		TermType type = TermType.getInstance(isMultiClient);
		Map<String, Byte> tcodes = new HashMap<String, Byte>();
		
		for ( int i=Byte.MIN_VALUE; i<= Byte.MAX_VALUE; i++) {
			type.autoInsert(tcodes, ("code" + i));
		}
		
		try {
			type.autoInsert(tcodes, "outrange");
		} catch (ApplicationFault ex) {
			assertEquals("com.bizosys.oneline.ApplicationFault: No type code slots available", ex.toString());
		}
	}
}
