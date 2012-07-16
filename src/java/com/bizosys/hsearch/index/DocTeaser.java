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
package com.bizosys.hsearch.index;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.filter.IStorable;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.schema.IOConstants;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;

/**
 * Documents could be coming as structured or unstructured in a unified platform.
 * So showing multiple document format results requires a standard way of accessing
 * the result display section. Teaser fields standardizes this. 
 * @author bizosys
 *
 */
public class DocTeaser  implements IStorable {

	/**
	 * The Document ID
	 */
	public String id =  null;
	
	/**
	 * Where is this document located
	 */
	public String url =  null;

	/**
	 * The title of the document
	 */
	public String title =  null; 
	
	/**
	 * Cached Text
	 */
	public String cacheText =  null;
	
	/**
	 * The body text of the document
	 */
	public String preview =  null;

	public DocTeaser() {
	}
	
	/**
	 * Formulates the document teaser from the supplied Fields of a document. 
	 * @param aDoc
	 */
	public DocTeaser(HDocument aDoc) throws ApplicationFault {
		this.id = aDoc.key;
		if ( null != aDoc.url) 
			this.url = StringEscapeUtils.escapeXml(aDoc.url);
		if ( null != aDoc.title) 
			this.title =   StringEscapeUtils.escapeXml(aDoc.title);
		if ( null != aDoc.cacheText ) 
			this.cacheText = StringEscapeUtils.escapeXml(aDoc.cacheText);
		if ( null != aDoc.preview ) {
			this.preview = StringEscapeUtils.escapeXml(aDoc.preview);
		}
	}

	public DocTeaser (byte[] data) throws ApplicationFault, SystemFault {
		this.fromBytes(data, 0);
	}
	
	public DocTeaser (byte[] id, byte[] data) throws ApplicationFault, SystemFault {
		this.id = Storable.getString(id);
		this.fromBytes(data, 0);
	}

	public void toNVs(List<NV> nvs) throws ApplicationFault, SystemFault{
		byte[] data = toBytes();
		nvs.add(new NV(IOConstants.TEASER_BYTES,IOConstants.TEASER_BYTES, new Storable(data)));
	}

	public void setUrl(String url) {
		if ( null == url) this.url = null;
		else this.url = url;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setCacheText(String bodyText) {
		if ( null != bodyText) bodyText = bodyText.trim();
		if ( null == bodyText) this.cacheText = null;
		else this.cacheText = bodyText;
	}
	
	public String getCachedText() {
		return this.cacheText;
	}
	
	public void setPreview(String preview) {
		this.preview = preview; 
	}
	
	public String getPreview() {
		return this.preview;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getTitle() {
		return this.title;
	}

	public byte[] toBytes() {
		
		int idLen=0,urlLen=0,titleLen=0,cacheTextLen=0, previewLen=0;
		byte[] idB=null,urlB=null,titleB=null,cacheTextB=null, previewB=null;
		if ( null != this.id) {
			idB = Storable.putString(this.id);
			idLen = ( null == idB ) ? 0 : idB.length;
		}
			
		if ( null != this.url) {
			urlB = Storable.putString(this.url);
			urlLen = ( null == urlB ) ? 0 : urlB.length;
		}

		if ( null != this.title) {
			titleB = Storable.putString(this.title);
			titleLen = ( null == titleB ) ? 0 : titleB.length;
		}

		if ( null != this.preview) {
			previewB = Storable.putString(this.preview);
			previewLen = ( null == previewB) ? 0 : previewB.length;
		}
		
		if ( null != this.cacheText) {
			cacheTextB = Storable.putString(this.cacheText);
			cacheTextLen = ( null == cacheTextB ) ? 0 : cacheTextB.length;
		}
		
		int dataSize = 14 + idLen + urlLen + titleLen + previewLen + cacheTextLen;
		byte[] data = new byte[dataSize];
		int index = 0;
		data[index++] = (byte)(idLen >> 8 & 0xff); 
		data[index++] = (byte)(idLen & 0xff); 
		
		data[index++] = (byte)(urlLen >> 8 & 0xff); 
		data[index++] = (byte)(urlLen & 0xff); 
		
		data[index++] = (byte)(titleLen >> 8 & 0xff); 
		data[index++] = (byte)(titleLen & 0xff); 

		data[index++] = (byte)(previewLen >> 24); 
		data[index++] = (byte)(previewLen >> 16 );
		data[index++] = (byte)(previewLen >> 8 );
		data[index++] = (byte)(previewLen);
		
		data[index++] = (byte)(cacheTextLen >> 24); 
		data[index++] = (byte)(cacheTextLen >> 16 );
		data[index++] = (byte)(cacheTextLen >> 8 );
		data[index++] = (byte)(cacheTextLen);

		if ( idLen > 0 ) {
			System.arraycopy(idB, 0, data, index, idLen);
			index = index  + idLen;
		}

		if ( urlLen > 0 ) {
			System.arraycopy(urlB, 0, data, index, urlLen);
			index = index  + urlLen;
		}

		if ( titleLen > 0 ) {
			System.arraycopy(titleB, 0, data, index, titleLen);
			index = index  + titleLen;
		}

		if ( previewLen > 0 ) {
			System.arraycopy(previewB, 0, data, index, previewLen);
			index = index  + previewLen;
		}
		
		if ( cacheTextLen > 0 ) {
			System.arraycopy(cacheTextB, 0, data, index, cacheTextLen);
			index = index  + cacheTextLen;
		}
		
		return data;
	}
		
	/**
	 * Clean up the entire set.
	 */
	public void cleanup() {
		this.id =  null;
		this.url =  null;
		this.title =  null; 
		this.cacheText =  null;
		this.preview =  null;
	}
	
	public void toXml(Writer pw) throws IOException {
		if ( null == pw) return;
		if ( null != id ) {
			pw.append('<').append(IOConstants.TEASER_ID).append('>');
			pw.append(id);
			pw.append("</").append(IOConstants.TEASER_ID).append('>');
		}
		
		if ( null != url ) {
			pw.append('<').append(IOConstants.TEASER_URL).append("><![CDATA[");
			pw.append( url.toString() );
			pw.append("]]></").append(IOConstants.TEASER_URL).append('>');
		}
		
		if ( null != title ) {
			pw.append('<').append(IOConstants.TEASER_TITLE).append("><![CDATA[");
			pw.append( title.toString() );
			pw.append("]]></").append(IOConstants.TEASER_TITLE).append('>');
		}

		if ( null != cacheText ) {
			pw.append('<').append(IOConstants.TEASER_CACHE).append("><![CDATA[");
			pw.append( cacheText.toString() );
			pw.append("]]></").append(IOConstants.TEASER_CACHE).append('>');
		}

		if ( null != preview ) {
			pw.append('<').append(IOConstants.TEASER_PREVIEW).append("><![CDATA[");
			pw.append( preview );
			pw.append("]]></").append(IOConstants.TEASER_PREVIEW).append('>');
		}
	}
	
	public String getId() {
		if ( null == id ) return "";
		return this.id;
	}
	
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder('\n');
		if ( null != id ) sb.append("] , Id : [").append(id);
		if ( null != url ) sb.append("] , Url : [").append(url);
		if ( null != title ) sb.append("] , Title : [").append(title);
		if ( null != cacheText ) sb.append("] , Body :[").append(cacheText);
		if ( null != preview ) sb.append("] , Preview :[").append(preview);
		sb.append(']');
		return sb.toString();
	}

	public int fromBytes(byte[] data, int bytePos) {
		if ( null == data )return bytePos;
		if ( data.length < 14 ) return bytePos;
		
		//Read a short.
		int idLen = Storable.getShort(bytePos, data);
		bytePos = bytePos + 2;
		
		int urlLen = Storable.getShort(bytePos, data);
		bytePos = bytePos + 2;

		int titleLen = Storable.getShort(bytePos, data);
		bytePos = bytePos + 2;
		
		int previewLen = Storable.getInt(bytePos, data);
		bytePos = bytePos + 4;
		
		int cacheLen = Storable.getInt(bytePos, data);
		bytePos = bytePos + 4;

		if ( idLen > 0) {
			byte[] idB = new byte[idLen];
			System.arraycopy(data, bytePos, idB, 0, idLen);
			this.id = new String(idB);
			bytePos = bytePos + idLen;
		}
		
		if ( urlLen > 0) {
			byte[] urlB = new byte[urlLen];  
			System.arraycopy(data, bytePos, urlB, 0, urlLen);
			this.url = new String(urlB);
			bytePos = bytePos + urlLen;
		}

		if ( titleLen > 0) {
			byte[] titleB = new byte[titleLen];
			System.arraycopy(data, bytePos, titleB, 0, titleLen);
			this.title = new String(titleB);
			bytePos = bytePos + titleLen;
		}	
		
		if ( previewLen > 0) {
			byte[] previewB = new byte[previewLen];  
			System.arraycopy(data, bytePos, previewB, 0, previewLen);
			this.preview = new String(previewB); 
			bytePos = bytePos + previewLen;
		}
		
		if ( cacheLen > 0) {
			byte[] cacheB = new byte[cacheLen];  
			System.arraycopy(data, bytePos, cacheB, 0, cacheLen);
			this.cacheText = new String(cacheB); 
			bytePos = bytePos + cacheLen;
		}
		return bytePos;
	}
}
