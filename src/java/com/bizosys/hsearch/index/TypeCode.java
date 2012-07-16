package com.bizosys.hsearch.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.hsearch.util.RecordScalar;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

public abstract class TypeCode {
	
	protected Map<String, Map<String, Byte>> tenantTypeCodes = 
		new ConcurrentHashMap<String, Map<String, Byte>>();

	
	/**
	 * For a given tenant and type code, it gives all the available type codes.
	 * @param tenant
	 * @param type
	 * @return
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	public Byte getTypeCode(String tenant, String type) throws ApplicationFault, SystemFault {
		if ( StringUtils.isEmpty(type)) return null;
		
		/** Tenant is cached and has the typecode */
		Map<String, Byte>  cachedCodes = null;
		if ( tenantTypeCodes.containsKey(tenant) ) {
			cachedCodes = tenantTypeCodes.get(tenant);
			if (cachedCodes.containsKey(type)) return cachedCodes.get(type);
		} 
		
		/** In any case make a fresh loading */
		cachedCodes = load(tenant);

		/** First time setting. Set it and return  */
		if ( null == cachedCodes ) {
			cachedCodes = getDefaultCodes();
			if ( null == cachedCodes ) cachedCodes =  new HashMap<String, Byte>();
		} 
		
		tenantTypeCodes.put(tenant, cachedCodes);

		/** Found in the fresh copy */
		if (cachedCodes.containsKey(type)) {
			return cachedCodes.get(type);
		}
		
		/** Auto Insert and serve */
		autoInsert(cachedCodes, type);
		persist(tenant, cachedCodes);
		return cachedCodes.get(type);
		
	}
	
	/**
	 * Finds a blank space and insert te new type code there.
	 * @param typeCodes
	 * @param newType
	 * @throws ApplicationFault
	 */
	public void autoInsert(Map<String, Byte> typeCodes, String newType) throws ApplicationFault {
		boolean[] filledPos = new boolean[256];
		Arrays.fill(filledPos, false);
		
		for (int b : typeCodes.values()) {
			b = b - Byte.MIN_VALUE;
			filledPos[b] = true;
		}
		
		//Take the immediate open position
		for ( int i=0; i<filledPos.length; i++) {
			if ( filledPos[i]) continue;
			
			typeCodes.put(newType, (byte) (i - Byte.MIN_VALUE));
			return;
		}
		throw new ApplicationFault ("No type code slots available");
	}
	
	protected Map<String, Byte> load(String tenant, byte[] key) throws SystemFault, ApplicationFault {
		NV nv = new NV(IOConstants.NAME_VALUE_BYTES, IOConstants.NAME_VALUE_BYTES);
		
		RecordScalar scalar = new RecordScalar(key, nv);
		HReader.getScalar(IOConstants.TABLE_CONFIG, scalar);
		if ( null == nv.data) return null; //Virgin, Nothing is set yet.
		
		byte[] bytes = nv.data.toBytes();
		int total = bytes.length;
		
		int pos = 0;
		byte len = 0;
		
		Map<String, Byte> newTypes = new HashMap<String, Byte>(256);
		while ( pos < total) {
			len =  bytes[pos++];
			if ( len < 1) continue;
			byte[] typeB = new byte[len];
			if ( (pos + len) > bytes.length ) {
				IndexLog.l.warn("TypeCode Loading Issue :" + 
					bytes.length + "/" + pos + "/" + len);
				break;
			}
			System.arraycopy(bytes, pos, typeB, 0,len);
			pos = pos + len;
			byte typeCode = bytes[pos++];
			newTypes.put(new String(typeB), typeCode);
		}
		return newTypes;
	}	
	
	public void persist(String tenant, Map<String, Byte> types, byte[] key) throws SystemFault {
		
		int totalSize = 0;
		try {
			for (String type : types.keySet()) {
				if ( StringUtils.isEmpty(type)) continue;
				totalSize = totalSize + 
					1 /** Type char length */  + type.length() + 1 /** Reserved for byte mapping */;  
			}
			if ( 0 == totalSize ) return;
			
			byte[] bytes = new byte[totalSize];
			
			int pos = 0, len = 0;
			for (String type : types.keySet()) {
				if ( StringUtils.isEmpty(type)) continue;
				len =  type.length();
				bytes[pos++] = (byte)len;
				System.arraycopy(type.getBytes(), 0, bytes, pos,len);
				pos = pos + len;
				bytes[pos++] = types.get(type);
			}
			NV nv = new NV(IOConstants.NAME_VALUE_BYTES, 
				IOConstants.NAME_VALUE_BYTES, new Storable(bytes));
			RecordScalar record = new RecordScalar(new Storable(key), nv);
			HWriter.getInstance(true).insertScalar(IOConstants.TABLE_CONFIG, record);
			
			tenantTypeCodes.put(tenant, types);
		} catch (IOException e) {
			IndexLog.l.fatal(toXml(types, "XXX"));
			throw new SystemFault(e);
		}
	}	
	
	public abstract void persist(String tenant, Map<String, Byte> types) throws SystemFault;
	public abstract Map<String, Byte> load(String tenant) throws SystemFault, ApplicationFault;
	public abstract void truncate(String tenant) throws SystemFault, ApplicationFault;
	public abstract Map<String, Byte> getDefaultCodes() throws SystemFault, ApplicationFault;
	
	protected String toXml(Map<String, Byte> codes, String type) {
		if ( null == codes) return "<codes></codes>";
		
		StringBuilder sb = new StringBuilder();
		sb.append("<codes>");
		if ( null != codes) {
			for (String termType: codes.keySet()) {
				sb.append('<').append(type).append('>');
				sb.append("<type>").append(termType).append("</type>");
				sb.append("<code>").append( (int) codes.get(termType)).append("</code>");
				sb.append("</").append(type).append('>');
			}
		}
		sb.append("</codes>");
		return sb.toString();
	}		
	
	public String toString(Map<String, Byte> codes) {
		
		StringBuilder sb = new StringBuilder();
		if ( null != codes) {
			for (String termType: codes.keySet()) {
				sb.append(termType).append('=');
				sb.append((int) codes.get(termType)).append('\n');
			}
		}
		return sb.toString();
	}	
	
	protected void deleteCode(String strKey) throws SystemFault {
		IStorable key = new Storable(strKey);
		try {
			HWriter.getInstance(true).delete(IOConstants.TABLE_CONFIG, key);
		} catch (IOException e) {
			throw new SystemFault(e);
		}
	}
}
