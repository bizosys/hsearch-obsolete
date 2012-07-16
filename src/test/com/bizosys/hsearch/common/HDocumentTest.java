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
package com.bizosys.hsearch.common;

import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.filter.Access;
import com.bizosys.hsearch.filter.Storable;
import com.bizosys.hsearch.index.TermType;
import com.bizosys.oneline.util.XmlUtils;
import com.thoughtworks.xstream.XStream;

public class HDocumentTest extends TestCase {

	public static void main(String[] args) throws Exception {
		//AccessControlTest t = new AccessControlTest();
        //TestFerrari.testAll(t);
		
		HDocument doc1 = new HDocument();
		doc1.key = "1";
		doc1.title = "Title : " ;
		doc1.cacheText = doc1.title + " " + "Adabra Cadabra";
		doc1.fields = new ArrayList<Field>();
		SField fld = new SField("BODY", "Adabra Cadabra SS");
		doc1.fields.add(fld);
		System.out.println( XmlUtils.xstream.toXML(doc1) );
		
	}
	private static final String TENANT = "ANONYMOUS";
	
	public void testHDoc() throws Exception {
		HDocument doc = new HDocument(TENANT);
		doc.bucketId = 99999L;
		doc.docSerialId = (short) 12;
		doc.cacheText = "This is cache text";
		doc.citationFrom = new ArrayList<String>();
		doc.citationFrom.add("Cited From paper1");
		doc.citationFrom.add("Cited From paper2");
		
		doc.citationTo = new ArrayList<String>();
		doc.citationTo.add("Cited To Paper1");
		doc.citationTo.add("Cited To Paper2");
		
		doc.createdOn = new Date();
		doc.docType = "doctype1";
		doc.weight = 11;
		doc.eastering = 100012.23F;
		doc.northing = 200012.23F;
		doc.editPermission = new AccessDefn();
		doc.editPermission.uids = new String[] {"n4501"};
		doc.editPermission.teams = new String[] {"teamA"};
		
		doc.fields = new ArrayList<Field>();
		doc.fields.add(new SField(true,true,true,Storable.BYTE_STRING,"fld1","value1"));
		doc.fields.add(new SField(true,true,true,Storable.BYTE_INT,"fld2","199"));
		
		doc.ipAddress = "192.168.2.3";
		doc.locale = Locale.ENGLISH;
		
		doc.modifiedOn = new Date();
		doc.northing = 23.44F;
		doc.key = "ORIG123";
		doc.preview = "<b>I am cool</b>";
		doc.securityHigh = false;
		doc.sentimentPositive = false;
		doc.socialText = new ArrayList<String>();
		doc.socialText.add("universe");
		doc.socialText.add("bob kamath");
		
		doc.state = "active";
		doc.tags = new ArrayList<String>();
		doc.tags.add("sociology");
		doc.tags.add("biology");
		
		doc.team = "bizosys";
		doc.title = "Title Text";
		doc.url = "http://wwww.google.com";
		
		doc.validTill = new Date();
		doc.viewPermission = new AccessDefn();
		doc.viewPermission.roles = new String[] {"Role1"};
		doc.viewPermission.uids = new String[] { Access.ANY };
		
		doc.weight = 99;
    	String xmlDoc = XmlUtils.xstream.toXML(doc);
		HDocument docDeserialized = (HDocument) 
			XmlUtils.xstream.fromXML(xmlDoc);
		
    	String xmlDocSerialized = XmlUtils.xstream.toXML(docDeserialized);
		
		System.out.println(xmlDoc);
		
		HDocument hdoc = new HDocument();
		hdoc.key = "001";
		hdoc.title = "This is my First Record";
		hdoc.fields = new ArrayList<Field>();
		hdoc.fields.add(new SField("A1", "Bangalore"));
    	String str001 = XmlUtils.xstream.toXML(hdoc);
    	System.out.println(str001);
    	
    	TermType tt = TermType.getInstance(true);
    	System.out.println( tt.toXml() );
		
    	
    	System.out.println("##################");
		HDocument doc1 = new HDocument();
		doc1.key = "1";
		doc1.title = "Hello World";
		doc1.cacheText = doc1.title ;
		doc1.fields = new ArrayList<Field>();
		HField fld = new HField("BODY", "Learn How to Search");
		doc1.fields.add(fld);
    	System.out.println(XmlUtils.xstream.toXML(doc1));
    	
	}
	
}
