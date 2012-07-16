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

import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.filter.AccessStorable;
import com.bizosys.hsearch.filter.Storable;

public class AccessControl {
	
	/**
	 * Form an access object out of a WhoAmI string
	 * @param whoami	The User identity
	 * @return	Access object
	 */
	public static final Access getAccessControl(WhoAmI whoami) {
		
		if ( null == whoami) return null;
		Access acl = new Access();
				
		boolean hasRoles = (null != whoami.roles);
		if ( hasRoles ) {
			for (String role : whoami.roles) {
				if ( ! StringUtils.isEmpty(role) ) acl.addRole(role);
			}
		}

		if ( null != whoami.teams ) {
			for (String team : whoami.teams) {
				if ( ! StringUtils.isEmpty(team) ) acl.addTeam(team); 
			}
		}
		
		boolean hasUid = ! StringUtils.isEmpty( whoami.uid);
		if ( hasUid ) acl.addUid(whoami.uid); 
		boolean hasOu = ! StringUtils.isEmpty( whoami.ou);
		if ( hasOu ) acl.addOrgUnit(whoami.ou); 
		if ( hasUid && hasOu ) acl.addOrgUnitAndUid(whoami.ou, whoami.uid);
		if ( hasOu && hasRoles ) {
			for (String role : whoami.roles) {
				if ( ! StringUtils.isEmpty(role) ) acl.addOrgUnitAndRole(whoami.ou, role);
			}
		}
		
		return acl;
	}
	
	/**
	 * Check for the available access for a user against the given access
	 * @param whoami	The user identity
	 * @param access	Access details
	 * @return	True if allowed
	 * @throws SystemFault
	 */
	public static boolean hasAccess (WhoAmI whoami, AccessStorable access) 
		throws SystemFault {

		Access acl = AccessControl.getAccessControl(whoami);
		AccessStorable userAcls = acl.toStorable();

		boolean allow = false;
		
		for (Object objFoundAcl : access) {
			byte[] foundAcl =  ((byte[]) objFoundAcl);
			
			if (Storable.compareBytes(foundAcl, Access.ANY_BYTES)) {
				allow = true; break;
			}
			
			for (Object userAcl : userAcls) {
				allow = Storable.compareBytes(foundAcl, (byte[]) userAcl);
				if ( allow ) break;
			}
			if ( allow ) break;
		}
		return allow;
	}
}
