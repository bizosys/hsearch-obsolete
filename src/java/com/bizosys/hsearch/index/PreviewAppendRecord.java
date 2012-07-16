package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.MergedBlocks;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.inpipe.InpipeLog;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.Record;

public class PreviewAppendRecord extends Record {

	Map<Integer, byte[]> metas = new HashMap<Integer, byte[]>();
	Map<Integer, byte[]> acls = new HashMap<Integer, byte[]>();
	Map<Integer, byte[]> teasers = new HashMap<Integer, byte[]>();
	
	byte[] metaH = null, metaD = null, aclH = null, 
		aclD = null, teaserH = null, teaserD = null;
	
	public PreviewAppendRecord(IStorable pk) {
		super(pk);
	}
	
	public PreviewAppendRecord(IStorable pk,Map<Integer, byte[]> metas,
		Map<Integer, byte[]> acls, Map<Integer, byte[]> teasers ) {
		super(pk);
		this.metas = metas;
		this.acls = acls;
		this.teasers = teasers;
	}
	
	@Override
	public List<NV> getBlankNVs() throws IOException {
		List<NV> nvs = new ArrayList<NV>(6);
		if ( null != metas) {
			nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.META_HEADER, null) ); 
			nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.META_DETAIL, null) );
		}
		if ( null != acls) {
			nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.ACL_HEADER, null ) );
			nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.ACL_DETAIL, null ) ); 
		}
		if ( null != teasers) {
			nvs.add(new NV(IOConstants.TEASER_BYTES, IOConstants.TEASER_HEADER, null ) ); 
			nvs.add(new NV(IOConstants.TEASER_BYTES, IOConstants.TEASER_DETAIL, null ) );
		}
		return nvs;		
	}
	
	
	@Override
	public boolean merge(byte[] fam, byte[] name, byte[] data) {
		switch ( name[0]) {
			case IOConstants.ACL_HEADER_0:
				if ( null != acls) aclH = data;
				break;
			case IOConstants.ACL_DETAIL_0:
				if ( null != acls) aclD = data;
				break;
			case IOConstants.META_HEADER_0:
				if ( null != metas) metaH = data;
				break;
			case IOConstants.META_DETAIL_0:
				if ( null != metas) metaD = data;
				break;
			case IOConstants.TEASER_HEADER_0:
				if ( null != teasers) teaserH = data;
				break;
			case IOConstants.TEASER_DETAIL_0:
				if ( null != teasers) teaserD = data;
				break;
			default:
				break;
		}
		return true;
	}
	
	List<NV> nvs = null;
	public List<NV> getNVs() throws IOException {
		if ( null == nvs) {
			nvs = new ArrayList<NV>(6);
			getNVs(nvs);
		}
		return nvs;
	}

	public List<NV> getNVs(List<NV> nvs) {
		/**
		 * Compute the Meta Merged Bytes
		 */
		if ( null != metas) {
			MergedBlocks.Block metaBlock = new MergedBlocks.Block(metaH,metaD);
			MergedBlocks.Block metaBlocknew = 
				MergedBlocks.merge(metaBlock, metas, new DocMeta());
			if ( null != metaBlocknew) {
				nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.META_HEADER, 
					new Storable(metaBlocknew.header) ));
				nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.META_DETAIL, 
					new Storable(metaBlocknew.data)));
			}
			metas.clear();
			if ( InpipeLog.l.isDebugEnabled() ) 
				InpipeLog.l.debug("PreviewAppendRecord: New Header:" + metaBlocknew.header.length );
		}
		
		/**
		 * Compute the ACL Bytes
		 */
		if ( null != acls) {
			MergedBlocks.Block aclBlock = new MergedBlocks.Block(aclH,aclD);
			MergedBlocks.Block aclBlocknew = 
				MergedBlocks.merge(aclBlock, acls, new DocAcl()); 
			if ( null != aclBlocknew) {
				nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.ACL_HEADER, new Storable(aclBlocknew.header)));
				nvs.add(new NV(IOConstants.SEARCH_BYTES, IOConstants.ACL_DETAIL, new Storable(aclBlocknew.data)));
			}
			acls.clear();
		}

		/**
		 * Compute the Teaser Bytes
		 */
		if ( null != teasers) {
			MergedBlocks.Block teaserBlock = new MergedBlocks.Block(teaserH,teaserD);
			MergedBlocks.Block teaserBlocknew = 
				MergedBlocks.merge(teaserBlock, teasers, new DocTeaser());
			if ( null != teaserBlocknew) {
				nvs.add(new NV(IOConstants.TEASER_BYTES, IOConstants.TEASER_HEADER, new Storable(teaserBlocknew.header)));
				nvs.add(new NV(IOConstants.TEASER_BYTES, IOConstants.TEASER_DETAIL, new Storable(teaserBlocknew.data)));
			}
			teasers.clear();
		}
		
		return nvs;
	}	
	
}
