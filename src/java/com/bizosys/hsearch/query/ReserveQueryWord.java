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

import java.util.HashMap;
import java.util.Map;

/**
 * Various reserve words for HSearch query 
 * <table>
 * <tr><td>page</td><td>page size</td></tr>
 * <tr><td>scroll</td><td>Scroll to record</td></tr>
 * <tr><td>typ</td><td>Find matching document type</td></tr>
 * <tr><td>ste</td><td>Find matching document State</td></tr>
 * <tr><td>team</td><td>Find matching document team</td></tr>
 * <tr><td>createdb</td><td>Find document created before date</td></tr>
 * <tr><td>createda</td><td>Find document created after date</td></tr>
 * <tr><td>modifieda</td><td>Find document modified before date</td></tr>
 * <tr><td>modifiedb</td><td>Find document odified after date</td></tr>
 * <tr><td>aikr</td><td>Scope to are in km radius</td></tr>
 * <tr><td>latlng</td><td>Provide the anchor Latitude and Longitude for finding</td></tr>

 * <tr><td>mfl</td><td>Number of documents on which meta ranking will be allowed</td></tr>
 * <tr><td>dfl</td><td>Number of documents to be fetched</td></tr>
 * <tr><td>tsl</td><td>Teaser section length of the record result</td></tr>
 * </table>
 * @author karan
 *
 */
public class ReserveQueryWord {
	private static ReserveQueryWord instance = null; 
	public static ReserveQueryWord getInstance() {
		if ( null != instance) return instance;
		instance = new ReserveQueryWord();
		return instance;
	}
	
	private Map<String, Integer> reserveWord = 
		new HashMap<String, Integer>();
	
	public static final int NO_RESERVE_WORD = -1;
	
	public static final int RESERVE_pagesize = 0;
	public static final int RESERVE_docType = 3;
	public static final int RESERVE_state = 4;
	public static final int RESERVE_team = 5;
	public static final int RESERVE_createdBefore = 6;
	public static final int RESERVE_createdAfter = 7;
	public static final int RESERVE_modifiedAfter = 8;
	public static final int RESERVE_modifiedBefore = 9;
	public static final int RESERVE_areaInKmRadius = 10;
	public static final int RESERVE_matchIp = 11;
	public static final int RESERVE_latlng = 12;
	public static final int RESERVE_scroll = 13;
	public static final int RESERVE_boostTermWeight = 14;
	public static final int RESERVE_boostDocumentWeight = 15;
	public static final int RESERVE_boostIpProximity = 16;
	public static final int RESERVE_boostOwner = 17;
	public static final int RESERVE_boostFreshness = 18;
	public static final int RESERVE_boostPrecious = 19;
	public static final int RESERVE_boostChoices = 20;
	public static final int RESERVE_boostMultiphrase = 21;
	public static final int RESERVE_facetFetchLimit = 22;
	public static final int RESERVE_metaFetchLimit = 23;
	public static final int RESERVE_documentFetchLimit = 24;	
	public static final int RESERVE_teaserSectionLength = 25;
	
	public static final int RESERVE_cluster = 100;	
	public static final int RESERVE_sortOnMeta = 101;	
	public static final int RESERVE_sortOnField = 102;	
	public static final int RESERVE_metaFields = 103;
	public static final int RESERVE_touchstones = 104;	

	private ReserveQueryWord() {
		reserveWord.put("page", RESERVE_pagesize);
		reserveWord.put("typ", RESERVE_docType);
		reserveWord.put("ste", RESERVE_state);
		reserveWord.put("team", RESERVE_team);
		reserveWord.put("createdb", RESERVE_createdBefore);
		reserveWord.put("createda", RESERVE_createdAfter);
		reserveWord.put("modifieda", RESERVE_modifiedAfter);
		reserveWord.put("modifiedb", RESERVE_modifiedBefore);
		reserveWord.put("aikr", RESERVE_areaInKmRadius);
		reserveWord.put("matchip", RESERVE_matchIp);
		reserveWord.put("latlng", RESERVE_latlng);
		reserveWord.put("scroll", RESERVE_scroll);
		reserveWord.put("termw", RESERVE_boostTermWeight);
		reserveWord.put("docbst", RESERVE_boostDocumentWeight);
		reserveWord.put("ipbst", RESERVE_boostIpProximity);
		reserveWord.put("ownerbst", RESERVE_boostOwner);
		reserveWord.put("freshbst", RESERVE_boostFreshness);
		reserveWord.put("preciousbst", RESERVE_boostPrecious);
		reserveWord.put("choicebst", RESERVE_boostChoices);
		reserveWord.put("mpbst", RESERVE_boostMultiphrase);
		reserveWord.put("ffl", RESERVE_facetFetchLimit);
		reserveWord.put("mfl", RESERVE_metaFetchLimit);
		reserveWord.put("dfl", RESERVE_documentFetchLimit);
		reserveWord.put("tsl", RESERVE_teaserSectionLength);
		
		reserveWord.put("cluster", RESERVE_cluster);
		reserveWord.put("som", RESERVE_sortOnMeta);
		reserveWord.put("sof", RESERVE_sortOnField);
		reserveWord.put("mf", RESERVE_metaFields);
		reserveWord.put("ts", RESERVE_touchstones);
	}
	
	/**
	 * Returns -1 is the word is not a reserved one.
	 * else returns the corresponding reserve word.
	 * @param word
	 * @return	Integer code (Helpful for avoiding if loops)
	 */
	public int mapReserveWord(String word) {
		if ( null == word) return NO_RESERVE_WORD;
		if ( reserveWord.containsKey(word)) {
			return reserveWord.get(word);
		} else {
			return NO_RESERVE_WORD;
		}
	}
}
