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
package com.bizosys.hsearch.filter;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.common.ByteField;
import com.bizosys.hsearch.common.StorableList;

public class StorableListTest extends TestCase {

	public static void main(String[] args) throws Exception {
		StorableListTest t = new StorableListTest();
        TestFerrari.testAll(t);
	}
	
	public void testAdd1String(String val1) {
		//System.out.println("val1 = " + val1);
		StorableList sl = new StorableList();
		sl.add(new ByteField("f1", val1));

		StorableList s2 = new StorableList( sl.toBytes() );
		for (Object object : s2) {
			assertEquals(val1, Storable.getString( (byte[])object));
		}
	}
	
	public void testAdd2String(String val1, String val2) {
		//System.out.println("val1 = " + val1 + " , val2 = " + val2);
		StorableList sl = new StorableList();
		sl.add(new ByteField("f1", val1));
		sl.add(new ByteField("f2", val2));

		StorableList s2 = new StorableList( sl.toBytes() );
		assertEquals(val1, Storable.getString( (byte[])s2.get(0)));
		assertEquals(val2, Storable.getString( (byte[])s2.get(1)));
	}
	
	public void testAddInteger(Integer val1, Integer val2) {
		//System.out.println("val1 = " + val1 + " , val2 = " + val2);
		StorableList sl = new StorableList();
		sl.add(new ByteField("f1", val1));
		sl.add(new ByteField("f2", val2));

		StorableList s2 = new StorableList( sl.toBytes() );
		assertEquals(val1.intValue(), Storable.getInt(0, (byte[])s2.get(0)));
		assertEquals(val2.intValue(), Storable.getInt(0, (byte[])s2.get(1)));
	}
	
	public void testSeek(String val1, String val2) {
		//System.out.println("val1 = " + val1 + " , val2 = " + val2);
		StorableList sl = new StorableList();
		sl.add(new ByteField("f1", val1));
		sl.add(new ByteField("f2", val2));

		byte[] a = new byte[100];
		a[0] = 34;
		a[1] = 33;
		a[2] = 36;

		byte[] i =  sl.toBytes();
		System.arraycopy( i, 0, a, 3, i.length);
		StorableList s2 = new StorableList( a,3,i.length);
		assertEquals(val1, Storable.getString( (byte[])s2.get(0)));
		assertEquals(val2, Storable.getString( (byte[])s2.get(1)));
	}
	
	
}
