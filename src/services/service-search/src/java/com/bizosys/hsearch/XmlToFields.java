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
package com.bizosys.hsearch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.bizosys.oneline.util.StringUtils;

public class XmlToFields extends DefaultHandler {
	
	StringBuilder sb = new StringBuilder(100);
	HashMap<String, String> xmlMap = null;
	
	String currentNode = null;
	String recordType = null;
	String fldSeparator = "   |   ";

	public XmlToFields () {
	}
	
	public XmlToFields (String fldSeparator) {
		this.fldSeparator = fldSeparator;
	}
	
	public HashMap<String, String> toMap( String xmlSnippet) {
		
		if ( null == xmlSnippet) return null;
		if ( "".equals(xmlSnippet.trim())) return null;
		
		if ( ! xmlSnippet.startsWith("<?xml")) xmlSnippet = 
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + xmlSnippet;
		
		SAXParserFactory spf = SAXParserFactory.newInstance();
		try {
			SAXParser sp = spf.newSAXParser();
			ByteArrayInputStream ba = new ByteArrayInputStream(xmlSnippet.getBytes());
			
			this.xmlMap = new HashMap<String, String>(24);
			this.sb.delete(0, sb.capacity());
			sp.parse(ba, this);
			return this.xmlMap;
			
		} catch(SAXException se) {
			se.printStackTrace();
			throw new IllegalArgumentException(se.getMessage());
		} catch(ParserConfigurationException pce) {
			pce.printStackTrace();
			throw new IllegalArgumentException(pce.getMessage());
		} catch (IOException ie) {
			ie.printStackTrace();
			throw new IllegalArgumentException(ie.getMessage());
		}
	}

	@Override
	public void startElement(String uri, String localName, 
			String qName, Attributes attributes) throws SAXException {
		
		if ( null == recordType) {
			recordType = qName;
		} else if ( StringUtils.isEmpty(currentNode) ) {
			currentNode =  qName;
		} else {
			currentNode = currentNode + "." + qName;
		}
		
		if ( null != attributes) {
			int attributesT = attributes.getLength();
			for ( int i=0; i< attributesT; i++) {
				String attName = attributes.getQName(i);
				String attVal = attributes.getValue(i);
				addToMap(attName, attVal);
			}
		}
	}
	
	@Override
	public void endElement(String uri, String localName,String qName) 
	throws SAXException {

		if ( qName.equals(recordType) ) return;
		
		String val = this.sb.toString().trim();
		this.sb.delete(0, sb.capacity());
		if ( ! StringUtils.isEmpty(val) ) addToMap(currentNode, val);
		
		if ( currentNode.endsWith(qName)) {
			int cutPos = currentNode.length() - qName.length();
			if ( cutPos > 0 ) {
				if ( currentNode.charAt(cutPos - 1) == '.') {
					cutPos = cutPos - 1;
				}
				currentNode = currentNode.substring(0, cutPos);
			} else if (cutPos  == 0 ) {
				currentNode = StringUtils.Empty;
			}
		} 
	}

	private void addToMap(String name, String val) {
		if ( this.xmlMap.containsKey(name)) {
			String existingVal = this.xmlMap.get(name);
			String newVal = existingVal + fldSeparator + val;
			this.xmlMap.put(name, newVal);
		} else {
			if (val.length() != 0 ) 
				this.xmlMap.put(name, val );
		}
	}
	
	@Override
	public void characters(char[] ch, int start, int length) {
		sb.append(ch, start, length);
	}
	
	public static void main(String[] args) throws Exception {
		XmlToFields xu = new XmlToFields();
		
		String strXml = "<book id=\"bk101\">	<author>Gambardella, Matthew</author>       <title>XML Developer's Guide</title>       <genre>Computer</genre>       <price>44.95</price>       <publish_date>2000-10-01</publish_date>       <description>An in-depth look at creating applications with XML.</description> <stores>		<store>store1</store>		<store>store2</store>	</stores> </book>";
		
		Map<String, String> ss = xu.toMap(strXml);
		if ( null == ss) return;
		for (String lside : ss.keySet()) {
			System.out.println(lside + "=" + ss.get(lside));
		}
		
	}	  
	
}