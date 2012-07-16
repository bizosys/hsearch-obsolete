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

import com.bizosys.hsearch.index.DocMeta;
import com.bizosys.oneline.SystemFault;

/**
 * Weighted document meta information.
 * 
 * @see DocMeta
 * @author karan
 *
 */
public class DocMetaWeight extends DocMeta {
	
	/**
	 * Bucket Id 
	 */
	public Long bucketId;
	
	/**
	 * Document Serial Id 
	 */
	public Short serialId;
	
	public float termWeight;
	
	public DocMetaWeight(Long bucketId, Short serialId, float termWeight) {
		super();
		this.bucketId = bucketId;
		this.serialId = serialId;
		this.termWeight = termWeight;
	}
	
	/**
	 * Constructor with byte value and record stacking information
	 * @param bucketId	Bucket Id
	 * @param serialId	Serial Id	
	 * @param metaBytes	Merged Bytes
	 */
	public DocMetaWeight(Long bucketId, Short serialId, byte[] metaBytes) {
		super(metaBytes);
		this.bucketId = bucketId;
		this.serialId = serialId;
	}
	
	/**
	 * Set the bucket and document serial numbers
	 * @param bucketId	Bucket Id
	 * @param serialId	Document Serial Number
	 */
	public void setIds(Long bucketId, Short serialId) {
		this.bucketId = bucketId;
		this.serialId = serialId;
	}
	
	public byte[] getIdBytes() {
		StringBuilder idB = new StringBuilder(20);
		idB.append(bucketId);
		idB.append('_'); 
		idB.append(serialId);
		return idB.toString().getBytes();
	}	
	
	/**
	 * Compare for sorting
	 * @param a Another <code>DocMetaWeight</code> object
	 * @return Equal = 0, Greater = -1 and Lesser = 1
	 */
	public int compare(DocMetaWeight a)  {
		if ( this.weight > a.weight) return -1;
		else if ( this.weight < a.weight) return 1;
		else return 0;
	}
	
	public static void  sort(Object[] out) throws SystemFault {
		if ( null == out) return;
		if ( 0 == out.length) return;
		try {
			sort ( out, 0, out.length -1 );
		} catch (Exception ex) {
			throw new SystemFault(ex);
		}
	}
	
	static DocMetaWeight temp = null;
	
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
            if (1 == ((DocMetaWeight)idWtL[low]).compare( (DocMetaWeight)idWtL[high] ) ) {
            	temp = (DocMetaWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
            return;
    	}

    	DocMetaWeight pivot =(DocMetaWeight) idWtL[(low + high) / 2];
        idWtL[(low + high) / 2] = idWtL[high];
        idWtL[high] = pivot;

        while( low < high ) {
            while ( ((DocMetaWeight)idWtL[low]).compare( pivot )  != 1  && low < high) low++;
            while (pivot.compare((DocMetaWeight)idWtL[high]) != 1 && low < high ) high--;
            if( low < high ) {
                temp = (DocMetaWeight)idWtL[low]; idWtL[low] = idWtL[high]; idWtL[high] = temp;
            }
        }

        idWtL[high0] = idWtL[high]; idWtL[high] = pivot;
    	sort(idWtL, low0, low-1);
    	sort(idWtL, high+1, high0);
	}
}
