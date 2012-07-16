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
package com.bizosys.hsearch.util;

import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.bizosys.oneline.util.StringUtils;

public class XmlRecordExtracter extends DefaultHandler
{
	private Map<String,String> fieldL = null;
	private StringBuilder fldValue = new StringBuilder(100);

	private String recordTag = null;
	private boolean isRecord = false;
	
	private XmlRecordExtracterCallback callback = null;
	
	private String currElement = "";
	
	public XmlRecordExtracter(String recordTag, XmlRecordExtracterCallback callback)
	{
		this.recordTag = recordTag;
		this.callback = callback;
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException
	{
		if (this.recordTag.equals(qName))
		{
			this.currElement = StringUtils.Empty;
			this.isRecord = true;
			fieldL = ObjectFactory.getInstance().getStringMap();
			return;
		}
		
		if (!this.isRecord) return;
		if (null != attributes)
		{
			int totalAttribs = attributes.getLength();
			for (int i = 0; i < totalAttribs; i++)
			{
				this.fieldL.put(attributes.getQName(i), attributes.getValue(i));
			}
		}
		
		this.currElement = this.currElement + qName;
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException
	{
		if (!this.isRecord) return;
		if (this.recordTag.equals(qName))
		{
			this.isRecord = false;
			if (this.callback.handleRecord(this.fieldL)) {
				return;
			}
			throw new SAXException("File processing is stopped as per application request.");
		}

		String strFldValue = this.fldValue.toString().trim();
		if (!StringUtils.isEmpty(strFldValue))
		{
			this.fieldL.put(this.currElement, strFldValue);
			this.fldValue.delete(0, this.fldValue.capacity());
		}
		this.currElement = this.currElement.substring(0, (this.currElement.length() - qName.length()));
	}

	@Override
	public void characters(char[] ch, int start, int length)
	{
		if (!this.isRecord) return;
		this.fldValue.append(ch, start, length);
	}
}
