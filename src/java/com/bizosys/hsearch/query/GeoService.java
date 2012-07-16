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
package com.bizosys.hsearch.query;


/**
/**
 * Resolves LatLng from a given IP address.
 *  
	CREATE TABLE  `geo`.`blocks` (
	  `startIp` decimal(10,0) NOT NULL,
	  `endIp` decimal(10,0) NOT NULL,
	  `locId` decimal(10,0) NOT NULL
	);
	
	com.oneline.is.dao.util.FileWriterUtil removeQuotes d:/downloads/GeoLiteCity-Blocks.csv  d:/downloads/GeoLiteCity-Blocks.csv.noquote	

 	LOAD DATA INFILE "D:/downloads/GeoLiteCity-Blocks.csv.noquote" INTO TABLE blocks
	FIELDS TERMINATED BY "," LINES TERMINATED BY '\r\n';
	
	---------
	CREATE TABLE  city (
	  locId decimal(10,0) NOT NULL,
	  country char(2) DEFAULT NULL,
	  region char(2) DEFAULT NULL,
	  city varchar(100) DEFAULT NULL,
	  postalCode char(10) DEFAULT NULL,
	  latitude float NOT NULL,
	  longitude float NOT NULL,
	  metroCode varchar(10) DEFAULT NULL,
	  areaCode varchar(10) DEFAULT NULL
	);
	
	
	select locid from geo.blocks where 2079165448 >= startIp AND  2079165448 <= endIp
	select latitude, longitude from city where locid = 20067
	
	 * @author karan
 */

public class GeoService {
	/**
	public static final String selectStmt = 
		"select latitude, longitude from city,blocks" + 
		" where city.locid = blocks.locid" + 
		" AND ? >= startIp  AND ? <= endIp";

	public static final String source = "geo"; 
	
	public Location getLocation(int ipHashed) throws Exception {
		
		List listObject = new ReadObject().execute(
			selectStmt, new Object[]{ipHashed, ipHashed}, Location.class);
		
		if ( null != listObject && listObject.size() == 1) {
			return (Location) listObject.get(0);
		}
		return null;
	}
	*/	
}
 