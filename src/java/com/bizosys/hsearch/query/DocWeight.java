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

import com.bizosys.oneline.SystemFault;

/**
 * Overall Term weights for a document. Top ranked term weighted 
 * documents enter for meta fetching and further refined 
 * meta and teaser ranking process.
 * @author karan
 *
 */
public class DocWeight {

	/**
	 * Bucket Id
	 */
	public Long bucketId;
	
	/**
	 * Serial Id
	 */
	public Short serialId;

	/**
	 * Document weight
	 */
	public float wt;

	/**
	 * Constructor
	 * @param bucketId	Bucket Id
	 * @param serialId	Document Serial Number inside the bucket
	 * @param wt	Weight
	 */
	public DocWeight(Long bucketId, Short serialId , float foundWt) {
		this.bucketId = bucketId;
		this.serialId = serialId;
		this.wt = foundWt;
	}
	
	public byte[] getIdBytes() {
		StringBuilder idB = new StringBuilder(20);
		idB.append(bucketId);
		idB.append('_'); 
		idB.append(serialId);
		return idB.toString().getBytes();
	}
	
	/**
	 * Adds the new found phrase
	 * @param addition
	 */
	public void add(float addition) {
		this.wt = this.wt * addition; 
	}
	
	public int compare(DocWeight a)  {
		if ( this.wt > a.wt) return -1;
		else if ( this.wt < a.wt) return 1;
		else return 0;
	}
	
	static DocWeight temp = null;

	/**
	 * Sort based on the weight in descentding order
	 * <lu>
	 * <li>This sort does not take extra memory for arraying</li>
	 * <li>It's relatively faster by </li>
	 * </lu> 
	 * @param out
	 * @throws Exception
	 */
	public static void  sort(Object[] out) throws SystemFault {
		if ( null == out) return;
		int outT = out.length;
		if ( 0 == outT) return;
		
		try {
			sort ( out, 0, outT -1 );
		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}
	
	/**
	 * Quicksort data elements based on weight
	 * @param idWtL 
	 * @param low0
	 * @param high0
	 * @throws Exception
	 */
	private static void  sort(Object idWtL[], 
			int low0, int high0) throws Exception {
		
    	int low = low0; int high = high0;
    	if (low >= high) return;
    	
        if( low == high - 1 ) {
            if (1 == ((DocWeight)idWtL[low]).compare( (DocWeight)idWtL[high] ) ) {
            	temp = (DocWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
            return;
    	}

    	DocWeight pivot =(DocWeight) idWtL[(low + high) / 2];
        idWtL[(low + high) / 2] = idWtL[high];
        idWtL[high] = pivot;

        while( low < high ) {
            while ( ((DocWeight)idWtL[low]).compare( pivot )  != 1  && low < high) low++;
            while (pivot.compare((DocWeight)idWtL[high]) != 1 && low < high ) high--;
            if( low < high ) {
                temp = (DocWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
        }

        idWtL[high0] = idWtL[high]; idWtL[high] = pivot;
    	sort(idWtL, low0, low-1);
    	sort(idWtL, high+1, high0);
	}
	
	@Override
	public String toString() {
		return "DocWeight > " + this.bucketId + "_" + this.serialId + "|" + this.wt;
	}
	
}
