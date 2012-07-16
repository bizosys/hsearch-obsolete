package com.bizosys.hsearch.index;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.IScanCallBack;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.SystemFault;

public class TenantDocuments implements IScanCallBack {
	private static final boolean DEBUG_ENABLED = IndexLog.l.isDebugEnabled();
	private List<String> tenantDocKeys = new ArrayList<String>();
	
	public List<String> getAllDocuments(String tenant) throws SystemFault {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
				IOConstants.NAME_VALUE_BYTES, null);
		HReader.getAllValues(IOConstants.TABLE_IDMAP, nv, tenant, this);
		
		if ( DEBUG_ENABLED) {
			int size = ( null == tenantDocKeys) ? 0 :tenantDocKeys.size();
			IndexLog.l.debug("Total Docs to purge :" + size);
		}
		return tenantDocKeys;
	}
	
	public void process(byte[] storedBytes) {
		if ( null == storedBytes) return;
		String mappedId = Storable.getString(storedBytes);
		tenantDocKeys.add(mappedId);
		if ( DEBUG_ENABLED) IndexLog.l.debug("Deleting Record :" + mappedId);
	}
}
