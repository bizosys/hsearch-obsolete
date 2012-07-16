package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.MergedBlocks;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.Record;

public class PreviewDeleteRecord extends Record {

	List<Short> docPos = new ArrayList<Short>();
	
	byte[] metaH = null, aclH = null, teaserH = null;
	
	public PreviewDeleteRecord(IStorable pk) {
		super(pk);
	}
	
	public PreviewDeleteRecord(IStorable pk, List<Short> docPos) {
		super(pk);
		this.docPos = docPos;
	}
	
	@Override
	public List<NV> getBlankNVs() throws IOException {
		List<NV> nvs = new ArrayList<NV>(3);
		nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.META_HEADER, null) ); 
		nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.ACL_HEADER, null ) );
		nvs.add(new NV(IOConstants.TEASER_BYTES, IOConstants.TEASER_HEADER, null ) ); 
		return nvs;		
	}	
	
	
	@Override
	public boolean merge(byte[] fam, byte[] name, byte[] data) {
		switch ( name[0]) {
			case IOConstants.ACL_HEADER_0:
				aclH = data;
				break;
			case IOConstants.META_HEADER_0:
				metaH = data;
				break;
			case IOConstants.TEASER_HEADER_0:
				teaserH = data;
				break;
			default:
				break;
		}
		return true;
	}
	
	public List<NV> getNVs() throws IOException {

		List<NV> nvs = new ArrayList<NV>(3);
		/**
		 * Compute the Meta Merged Bytes
		 */
		for (Short pos : docPos) {
			if ( null != metaH) {
				MergedBlocks.delete(metaH, pos);
			}
			if ( null != aclH) MergedBlocks.delete(aclH, pos);
			if ( null != teaserH) MergedBlocks.delete(teaserH, pos);
		}
		
		nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.ACL_HEADER, new Storable(aclH)));
		nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.META_HEADER, new Storable(metaH)));
		nvs.add(new NV(IOConstants.TEASER_BYTES, IOConstants.TEASER_HEADER, new Storable(teaserH)));
		return nvs;
	}	
	
}
