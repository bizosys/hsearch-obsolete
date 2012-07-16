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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.index.DocAcl;
import com.bizosys.hsearch.index.DocMeta;

public class AMFilterCommonTest extends TestCase {

	public static void main(String[] args) throws Exception {
		AMFilterCommonTest t = new AMFilterCommonTest();
        TestFerrari.testRandom(t);
	}
	
	public void testAccessSerialization() throws Exception {
		Access acl = new Access();
		acl.addOrgUnit("com.si");
		AccessStorable viewA = acl.toStorable();
		AMFilterCommon fma = new AMFilterCommon(
			viewA,null,null,null,-1L,-1L,-1L,-1L);

		AMFilterCommon fmaN = new AMFilterCommon();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
	}
	
	public void testCreationSerialization() throws Exception {
		AMFilterCommon fma = new AMFilterCommon(
			null,null,null,null,new Long(System.currentTimeMillis()),1000,-1L,-1L);

		AMFilterCommon fmaN = new AMFilterCommon();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
		
		assertTrue(  System.currentTimeMillis() - fmaN.maxCreationDate < 60000 );
		assertEquals(1000, fmaN.minCreationDate );
		assertEquals(-1,  fmaN.minModificationDate );
		assertEquals(-1,  fmaN.maxModificationDate  );
	}

	public void testModifiedSerialization() throws Exception {
		AMFilterCommon fma = new AMFilterCommon(
			null,null,null,null,-1L,-1L,new Long(System.currentTimeMillis()),1000);

		AMFilterCommon fmaN = new AMFilterCommon();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
		
		assertEquals(-1,  fmaN.minCreationDate);
		assertEquals(-1,  fmaN.maxCreationDate  );
		assertTrue(  System.currentTimeMillis() - fmaN.maxModificationDate < 60000 );
		assertEquals(1000, fmaN.minModificationDate );
	}
	
	public void testAllSerialization(String role, String keyword, String state, 
			String team, Long cb, Long ca, Long mb, Long ma) throws Exception {
		
		Access acl = new Access();
		acl.addRole(role);
		AccessStorable viewA = acl.toStorable();
		AMFilterCommon fma = new AMFilterCommon(
			viewA,new Storable(keyword).toBytes(), new Storable(state).toBytes(),
			new Storable(team).toBytes(), cb,ca,mb,ma);

		AMFilterCommon fmaN = new AMFilterCommon();
		fmaN.bytesA = fma.bytesA;
		fmaN.deserialize();
		assertEquals(keyword,  new String(fmaN.keyword));
		assertEquals(team,  new String(fmaN.team));
		assertEquals(state,  new String(fmaN.state));
		assertEquals(ca.longValue(),  fmaN.minCreationDate);
		assertEquals(cb.longValue(),  fmaN.maxCreationDate  );
		assertEquals(ma.longValue(),  fmaN.minModificationDate );
		assertEquals(mb.longValue(),  fmaN.maxModificationDate );
	}
	
	public void testFilterAcl(String aUnit, String anotherUnit) throws Exception {
		
		Access acl = new Access();
		acl.addOrgUnit(aUnit);
		AccessStorable viewA = acl.toStorable();
		
		AMFilterCommon fma = new AMFilterCommon(
			viewA,null,null,null,-1L,-1L,-1L,-1L);
		fma.deserialize();
		

		Access va = new Access();
		va.addOrgUnit(aUnit);
		DocAcl docAcl = new DocAcl(va,null);  
		assertTrue( fma.allowAccess(docAcl.toBytes(), 0) != -1);
		
		Access vb = new Access();
		vb.addOrgUnit(anotherUnit);
		assertTrue( fma.allowAccess(new DocAcl(vb,null).toBytes(), 0) == -1);
	}
	
	public void testFilterMeta(String keyword, String state, 
			String team, Long cb, Long ca, Long mb, Long ma) throws Exception {
		if ( cb < 0) cb = cb * -1;
		if ( ca < 0) ca = ca * -1;
		if ( mb < 0) mb = mb * -1;
		if ( ma < 0) ma = ma * -1;
		
		if ( cb > ca) {
			long temp = cb;
			cb = ca;
			ca = temp;
		}
		
		if ( mb > ma) {
			long temp = mb;
			mb = ma;
			ma = temp;
		}

		AMFilterCommon fma = new AMFilterCommon(
			null,new Storable(keyword).toBytes(), new Storable(state).toBytes(),
			new Storable(team).toBytes(), cb,ca,mb,ma);
		fma.deserialize();
		
		DocMeta  dm = new DocMeta();
		dm.state =  state;
		dm.team =  team;
		List<String> tagL = new ArrayList<String>();
		tagL.add(keyword);
		tagL.add("ram");
		dm.addTags(tagL);
		dm.createdOn =  new  Date( cb + ( (ca-cb) / 2) );
		dm.modifiedOn =  new  Date( mb + ( (ma-mb) / 2) );
		assertTrue(fma.allowMeta(dm.toBytes(),0) != -1);
		
	}
	
}
