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

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

import com.bizosys.hsearch.facet.IFacetField;
import com.bizosys.oneline.SystemFault;

public class QueryResult {
	
	/**
	 * DocWeight Array
	 */
	public Object[] sortedStaticWeights = null;
	
	/**
	 * DocMetaWeight Array
	 */
	public Object[] sortedDynamicWeights = null;
	
	/**
	 * DocTeaserWeight Array
	 */
	public Object[] teasers = null;
	
	public Map<String, IFacetField[]> facets = null;
	
	public void toXml(Writer writer) throws SystemFault {
		if ( null == teasers) return;
		
		try {
			writer.append("\n<result>");
			int docIndex = 0;
			for (Object teaserO : this.teasers) {
				writer.append("\n<doc>");
				writer.append("<index>" + docIndex++ + "</index>");
				DocTeaserWeight dtw = (DocTeaserWeight) teaserO;
				writer.append("<weight>" + dtw.weight + "</weight>");
				dtw.toXml(writer);
				writer.append("</doc>");
			}
			if ( null != this.facets) {
				for (String key : this.facets.keySet()) {
					writer.append('<').append(key).append('>');
					for (IFacetField facet : this.facets.get(key)) {
						writer.append(facet.toXml());
					}
					writer.append("</").append(key).append('>');
				}
			} 			
			writer.append("</result>");
		} catch (IOException e) {
			QueryLog.l.error("Error in preparing result output.", e);
			throw new SystemFault("QueryResult::toXml", e);
		}

	}
	
	@Override
	public String toString() {
		Writer spw = new StringWriter();
		
		try {
			if ( null == this.sortedStaticWeights) {
				spw.write("\nSorted Static Weights = 0 ");
			} else {
				float maxVal=0, minVal=0;
				for (Object sw : this.sortedStaticWeights) {
					DocWeight dw = (DocWeight) sw;
					if ( dw.wt > maxVal) maxVal = dw.wt;
					if ( dw.wt < minVal) minVal = dw.wt;
					
				}
				spw.write("\nSorted Static Weights : " + 
					this.sortedStaticWeights.length + " max/min=" + maxVal + "/" + minVal);
			}
			
			if ( null == this.sortedDynamicWeights) {
				spw.write("\nSorted Dynamic Weights = 0");
			} else {
				float maxVal=0, minVal=0;
				for (Object sw : this.sortedDynamicWeights) {
					DocMetaWeight dw = (DocMetaWeight) sw;
					if ( dw.weight > maxVal) maxVal = dw.weight;
					if ( dw.weight < minVal) minVal = dw.weight;
					
				}
				spw.write("\nSorted Dynamic Weights : " + 
					this.sortedDynamicWeights.length + " max/min=" + maxVal + "/" + minVal);
			}
			
			if ( null != this.facets) {
				for (String key : this.facets.keySet()) {
					spw.write("\nFacets = " + key );
					for (IFacetField facet : this.facets.get(key)) {
						spw.write("\nFacet = " + facet.toXml());
					}
				}
			} 

			toXml(spw);
			return spw.toString();
		} catch (Exception ex) {
			return ex.getMessage();
		}
	}
	
	public void cleanup() {
		sortedStaticWeights = null;  
		this.sortedDynamicWeights = null;
		this.teasers = null;
	}
}
