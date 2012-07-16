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
package com.bizosys.hsearch.common;

import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.filter.AccessStorable;

public class AccessControlTest extends TestCase {

	public static void main(String[] args) throws Exception {
		AccessControlTest t = new AccessControlTest();
        TestFerrari.testAll(t);
	}
	
		//Set user
	public void testUser(String uid) throws Exception {
		WhoAmI firstUser = new WhoAmI();
		firstUser.uid = uid;
		
		//Serialize access
		Access access = new Access();
		access.addUid(uid);
		AccessStorable myAccess = access.toStorable();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessStorable accessBytes = setAccess.toStorable(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.uid = "XYZ";

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
	}
	
	public void testRole(String role) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.roles = new String[]{role};
		
		//Serialize access
		Access access = new Access();
		access.addRole(role);
		AccessStorable myAccess = access.toStorable();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessStorable accessBytes = setAccess.toStorable(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.uid = "XYZ";

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
		secondUser.roles = new String[]{"ROLE1","ROLE2"};
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
		secondUser.roles = new String[]{"ROLE1","ROLE2", role};
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));
	}
	
	public void testRoles(String role1, String role2,String role3) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.roles = new String[] {role1};
		
		//Serialize access
		Access access = new Access();
		access.addRole(role1);
		access.addRole(role2);
		access.addRole(role3);
		AccessStorable myAccess = access.toStorable();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessStorable accessBytes = setAccess.toStorable(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.roles = new String[] {role2};
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));

		//Check access
		WhoAmI thirdUser = new WhoAmI();
		thirdUser.roles = new String[] {role3};
		Assert.assertTrue(AccessControl.hasAccess(thirdUser, accessBytes));

		WhoAmI unknownUser = new WhoAmI();
		unknownUser.roles = new String[] {"XX"};
		Assert.assertFalse(AccessControl.hasAccess(unknownUser, accessBytes));
	}		
	
	public void testOrgUnit(String unit) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.ou = unit;
		
		//Serialize access
		Access access = new Access();
		access.addOrgUnit(unit);
		AccessStorable myAccess = access.toStorable();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessStorable accessBytes = setAccess.toStorable(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.uid = "XYZ";
		secondUser.ou = "XYZ";

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
		
		secondUser.ou = unit;
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));
		
	}
	
	public void testTeam(String team) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.teams = new String[] {team};
		
		//Serialize access
		Access access = new Access();
		access.addTeam(team);
		AccessStorable myAccess = access.toStorable();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessStorable accessBytes = setAccess.toStorable(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.teams = new String[] {"XYZ"};

		//Check access
		Assert.assertFalse(AccessControl.hasAccess(secondUser, accessBytes));
	}
	
	public void testTeams(String team1, String team2,String team3) throws Exception {
		//Set user
		WhoAmI firstUser = new WhoAmI();
		firstUser.teams = new String[] {team1};
		
		//Serialize access
		Access access = new Access();
		access.addTeam(team1);
		access.addTeam(team2);
		access.addTeam(team3);
		AccessStorable myAccess = access.toStorable();
		byte[] persist = myAccess.toBytes();
		
		//Deserialize access
		Access setAccess = new Access(persist);
		AccessStorable accessBytes = setAccess.toStorable(); 
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(firstUser, accessBytes));
		
		WhoAmI secondUser = new WhoAmI();
		secondUser.teams = new String[] {team2};
		Assert.assertTrue(AccessControl.hasAccess(secondUser, accessBytes));

		//Check access
		WhoAmI thirdUser = new WhoAmI();
		thirdUser.teams = new String[] {team3};
		Assert.assertTrue(AccessControl.hasAccess(thirdUser, accessBytes));

		WhoAmI unknownUser = new WhoAmI();
		unknownUser.teams = new String[] {"XX"};
		Assert.assertFalse(AccessControl.hasAccess(unknownUser, accessBytes));
	}	

	public void testIBMArchitectOnly(String unknownUnit, String unknownRole) throws Exception {
		WhoAmI ibmArchitect = new WhoAmI("ibm","architect");

		Access access = new Access();
		access.addOrgUnitAndRole("ibm","architect");

		//Check access
		Assert.assertTrue(AccessControl.hasAccess(
			ibmArchitect, access.toStorable()));
		
		Assert.assertFalse(AccessControl.hasAccess(
			new WhoAmI(unknownUnit,unknownRole), access.toStorable()));
		
	}
	
	public void testIITAlumniAndBizosys(String unknownUnit, String unknownTeam) throws Exception {
		WhoAmI iitAlumniAndBizosys = new WhoAmI();
		iitAlumniAndBizosys.ou = "bizosys";
		iitAlumniAndBizosys.teams = new String[]{"iit"};

		Access access = new Access();
		access.addOrgUnit("bizosys");
		access.addTeam("iit");
		
		//Check access
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));
		
		access.clear();
		access.addOrgUnit("bizosys");
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));
		
		access.clear();
		access.addTeam("iit");
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));

		access.clear();
		access.addOrgUnit(unknownUnit);
		access.addTeam("iit");
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));

		access.clear();
		access.addOrgUnit("bizosys");
		access.addTeam(unknownTeam);
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));

		access.clear();
		access.addOrgUnit(unknownUnit);
		access.addOrgUnit("bizosys");
		access.addTeam(unknownTeam);
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));

		access.clear();
		access.addOrgUnit(unknownUnit);
		access.addTeam("iit");
		access.addTeam(unknownTeam);
		Assert.assertTrue(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));

		/**
		 * False
		 */
		access.clear();
		access.addOrgUnit("iit");
		access.addTeam("bizosys");
		Assert.assertFalse(AccessControl.hasAccess( //Reversed
			iitAlumniAndBizosys, access.toStorable()));

		access.clear();
		access.addOrgUnit("iit");
		access.addTeam(unknownTeam);
		Assert.assertFalse(AccessControl.hasAccess(
			iitAlumniAndBizosys, access.toStorable()));

		access.clear();
		Assert.assertFalse(AccessControl.hasAccess(
			new WhoAmI(unknownUnit,unknownTeam), access.toStorable()));
		
	}

	public void testAnonymous() throws Exception {
		WhoAmI student = new WhoAmI("n-4501");
		Access access = new Access();
		access.addAnonymous();
		Assert.assertTrue(AccessControl.hasAccess(
				student, access.toStorable()));
		
	}
}
