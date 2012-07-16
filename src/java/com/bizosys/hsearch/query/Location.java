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

public class Location {

	public Integer locId;
	public String country;
	public String region;
	public String city;
	public String postalCode;
	public Float latitude;
	public Float longitude;
	public String metroCode;
	public String areaCode;

	/** Default constructor */
	public Location() {
	}


	/** Constructor with primary keys (Insert with primary key)*/
	public Location(Integer locId,String country,String region,String city,
		String postalCode,Float latitude,Float longitude,
		String metroCode,String areaCode) {

		this.locId = locId;
		this.country = country;
		this.region = region;
		this.city = city;
		this.postalCode = postalCode;
		this.latitude = latitude;
		this.longitude = longitude;
		this.metroCode = metroCode;
		this.areaCode = areaCode;

	}


	/** Params for (Insert with autoincrement)*/
	public Object[] getNewPrint() {
		return new Object[] {
			locId, country, region, city, postalCode, latitude, 
			longitude, metroCode, areaCode
		};
	}


	/** Params for (Insert with primary key)*/
	public Object[] getNewPrintWithPK() {
		return new Object[] {
			locId, country, region, city, postalCode, latitude, 
			longitude, metroCode, areaCode
		};
	}


	/** Params for (Update)*/
	public Object[] getExistingPrint() {
		return new Object[] {
			locId, country, region, city, postalCode, latitude, 
			longitude, metroCode, areaCode
		};
	}

}
