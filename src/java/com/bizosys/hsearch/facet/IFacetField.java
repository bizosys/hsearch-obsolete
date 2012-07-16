package com.bizosys.hsearch.facet;

public interface IFacetField {
	String toXml();

	String getFacet();
	int getSize();
	String getDocIds();
}
