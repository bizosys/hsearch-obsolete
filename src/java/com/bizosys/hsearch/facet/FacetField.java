package com.bizosys.hsearch.facet;

import org.apache.commons.lang.StringEscapeUtils;

public class FacetField implements IFacetField {

	public String facet = null;
	public int size = 1;
	public String docIds = null;
	
	public FacetField(String facet) {
		this.facet = facet;
	}
	
	public FacetField(String facet, int size) {
		this(facet, size, null);
	}

	public FacetField(String facet, int size, String docIds) {
		this.facet = facet;
		this.size = size;
		this.docIds = docIds;
	}
	
	public String getFacet() {
		return this.facet;
	}

	public int getSize() {
		return this.size;
	}
	

	public String getDocIds() {
		return this.docIds.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\t\t\t").append(this.facet).append(':').append(this.size);
		return sb.toString();
	}

	public String toXml()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("<f>");
		sb.append("<v>").append(StringEscapeUtils.escapeXml(this.facet)).append("</v>");
		sb.append("<c>").append(this.size).append("</c>");
		sb.append("<i>").append(this.docIds).append("</i>");
		sb.append("</f>");
		return sb.toString();
	}
}