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

import com.bizosys.oneline.util.StringUtils;

/**
 * Currently access can be set by defining combination of 
 * <lu>
 * 	<li>uid</li>
 * 	<li>team</li>
 * 	<li>role</li>
 * 	<li>org unit</li>
 * 	<li>orgunit + uid </li>
 * 	<li>orgunit + role</li>
 * <lu> 
 * TODO:: Allow the regular expression here.
 * @author karan
 *
 */
public class Access {

	/**
	 * Type Code for User Id
	 */
	private static final char UIDC = '0';

	/**
	 * Type Code for Team
	 */
	private static final char TEAMC = '1';

	/**
	 * Type Code for Role
	 */
	private static final char ROLEC = '2';

	/**
	 * Type Code for Organization Unit
	 */
	private static final char OUC = '3';

	/**
	 * Type Code for Organization Unit and User Id combination
	 */
	private static final char OU_UIDC = '4';

	/**
	 * Type Code for Organization Unit and Role combination
	 */
	private static final char OU_ROLEC = '5';

	/**
	 * Anonymous access
	 */
	public static final String ANY = "*";
	
	/**
	 * Anonymous access byte codes
	 */
	public static final byte[] ANY_BYTES = "0*".getBytes();
	 
	/**
	 * All access settings serialized to byte array using Serializable list
	 */ 
	AccessStorable storable = new AccessStorable();
	 
	/**
	 * Default Constructor
	 *
	 */
	public Access() {
	}
	 
	/**
	 * Deserialize the access codes from bytes-array
	 * @param bytes	Stored bytes
	 */
	public Access(byte[] bytes) {
		this.storable = new AccessStorable(bytes);
	}
	 
	/**
	 * Add anonymous access
	 */
	public void addAnonymous() {
		storable.add((UIDC + ANY).getBytes() );
	}
	
	/**
	 *	Give Access to an user
	 * @param uid	User Id
	 */
	public void addUid(String uid) {
		storable.add((UIDC + uid).getBytes() );
	}
	
	/**
	 * Give access to a role
	 * @param role	The Role
	 */
	public void addRole(String role) {
		storable.add((ROLEC + role).getBytes() );
	}

	/**
	 * Give access to a team
	 * @param team	The Team
	 */
	public void addTeam(String team) {
		storable.add((TEAMC + team).getBytes() );
	}

	/**
	 * Give access to a organization unit or a tenant.
	 * E.g. com.bizosys.india.bangalore
	 * @param ou	Organization unit / Tenant
	 */
	public void addOrgUnit(String ou) {
		storable.add((OUC + ou).getBytes() );
	}

	/**
	 * Give access to a specific user of a specific Organization Unit or Tenant
	 * @param ou	The Organization Unit 
	 * @param uid	The User Id
	 */
	public void addOrgUnitAndUid(String ou, String uid) {
		storable.add((OU_UIDC + ou + "." + uid).getBytes() );
	}
	
	/**
	 * Give access to a specific role of a specific Organization Unit or Tenant
	 * @param ou	Organization Unit / Tenant
	 * @param role	The Role name
	 */
	public void addOrgUnitAndRole(String ou, String role) {
		storable.add((OU_ROLEC + ou + "." + role).getBytes() );
	}
	
	/**
	 * The Serializable list
	 * @return	Storable Access List 
	 */
	public AccessStorable toStorable() {
		return storable;
	}
	
	/**
	 * Clear all the access settings
	 */
	public void clear() {
		if ( null != this.storable) this.storable.clear();
	}
	
	public String toString() {
		if ( null == storable.container) return StringUtils.Empty;
		
		StringBuilder sb = new StringBuilder();
		for (byte[] aRow : storable.container) {
			if ( aRow.length < 1) continue;

			sb.append("<allow>");
			char first = (char) aRow[0];
			switch(first) {
				case UIDC:
					sb.append("<uid>").append(new String(aRow,1,aRow.length-1)).append("</uid>");
					break;
				case TEAMC:
					sb.append("<team>").append(new String(aRow,1,aRow.length-1)).append("</team>");
					break;
				case ROLEC:
					sb.append("<role>").append(new String(aRow,1,aRow.length-1)).append("</role>");
					break;
				case OUC:
					sb.append("<unit>").append(new String(aRow,1,aRow.length-1)).append("</unit>");
					break;
				case OU_UIDC:
					sb.append("<unituid>").append(new String(aRow,1,aRow.length-1)).append("</unituid>");
					break;
				case OU_ROLEC:
					sb.append("<unitrole>").append(new String(aRow,1,aRow.length-1)).append("</unitrole>");
					break;
				default:
					sb.append("<unknown>").append(new String(aRow,1,aRow.length-1)).append("</unknown>");
					break;
			}
			sb.append("</allow>");
		}
		return sb.toString();
	}
}