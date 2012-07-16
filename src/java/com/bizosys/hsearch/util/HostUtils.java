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

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.bizosys.oneline.util.StringUtils;

/**
 * Provides running host identity. IP address and Host name
 * 
 * @author karan
 *
 */
public class HostUtils {
	private static String HOST_NAME = null;
	private static String IP = null;
	
	public static final String getHostName() {
		if ( null != HOST_NAME) return HOST_NAME;
		try {
			InetAddress addr = InetAddress.getLocalHost();
			HOST_NAME = addr.getHostName();
		} catch (UnknownHostException ex) {
			HOST_NAME = "localhost";
		}
		if ( StringUtils.isEmpty(HOST_NAME)) HOST_NAME = "localhost";
		return HOST_NAME;
	}

	public static String getIp() {
		if ( null != IP) return IP;
		try {
			InetAddress addr = InetAddress.getLocalHost();
			IP = addr.getHostAddress();
		} catch (UnknownHostException ex) {
			IP = "127.0.0.1";
		}
		if ( StringUtils.isEmpty(IP)) IP = "127.0.0.1";
		return IP;
	}

}
