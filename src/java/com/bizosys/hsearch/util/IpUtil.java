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
package com.bizosys.hsearch.util;

import com.bizosys.oneline.util.StringUtils;

/**
 * Represent the IP address to a unique integer.
 * @author Abinasha Karana
 *
 */
public class IpUtil {
	
	/**
	 * From a.b.c.d ip address it gives a unique number 
	 * @param strIp	a.b.c.d
	 * @return	IP House Code
	 */
	public static final int computeHouse(String strIp) {
		int ipHashed = 0;
		String[] ipAddrDivided = StringUtils.getStrings(strIp, ".");
		if ( ipAddrDivided.length == 4) {
			int a = new Integer( ipAddrDivided[0] );
			int b = new Integer( ipAddrDivided[1] );
			int c = new Integer( ipAddrDivided[2] );
			int d = new Integer( ipAddrDivided[3] );
			ipHashed = a * 16777216 + b * 65536  + c * 256 + d;
		}
		return ipHashed;
	}

}
