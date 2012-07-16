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

import com.bizosys.hsearch.filter.Access;

/**
 * Carries all access information. It is serializable across wire (REST API).
 * @author karan
 *
 */
public class AccessDefn {
	/**
	 * The User Ids
	 */
	public String[] uids = null;
	
	/**
	 * The Teams
	 */
	public String[] teams = null;
	
	/**
	 * The Roles
	 */
	public String[] roles = null;

	/**
	 * The Organization Units / Tenants
	 */
	public String[] ous = null;
	
	/**
	 * Specific Users from Organization Units 
	 */
	public String[][] ouAndUids = null;

	/**
	 * Specific Roles from Organization Units
	 */
	public String[][] ouAndRoles = null;
	
	/**
	 * Creates an Access Object from the provided access definition 
	 * @return	Access Object
	 */
	public Access getAccess() {
		
		if ( null == uids && null == teams && null == roles &&
			null == ous && null == ouAndUids && null == ouAndRoles) return null;
		
		Access access = new Access();
		if (null != uids) for (String uid : uids) access.addUid(uid);
		if (null != teams) for (String team : teams) access.addTeam(team);
		if (null != roles) for (String role : roles) access.addRole(role);
		if (null != ous) for (String ou : ous) access.addOrgUnit(ou);
		if (null != ouAndUids) for (String[] ouU : ouAndUids) access.addOrgUnitAndUid(ouU[0], ouU[1]);
		if (null != ouAndRoles) for (String[] ouR : ouAndRoles) access.addOrgUnitAndRole(ouR[0], ouR[1]);
		
		return access;
	}

}
