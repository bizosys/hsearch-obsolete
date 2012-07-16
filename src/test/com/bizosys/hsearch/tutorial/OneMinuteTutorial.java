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
package com.bizosys.hsearch.tutorial;

import junit.framework.TestCase;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.index.DocTeaser;
import com.bizosys.hsearch.index.IndexReader;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.query.QueryContext;
import com.bizosys.hsearch.query.QueryResult;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.ServiceFactory;


public class OneMinuteTutorial extends TestCase {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(); //Load configuration file (site.xml)
		ServiceFactory.getInstance().init(conf, null); //Initialize all services
		OneMinuteTutorial.testHello("BIZOSYS-001", "Welcome to hello world", "hello"); //Run Helloworld
	}

	public static void testHello(String id, String title, String query) throws Exception  {
		AccountInfo acc = null;
		IndexWriter.getInstance().insert(
			new HDocument(id, title), acc, false); //Index a HDocument
		QueryContext ctx = new QueryContext(acc, query); //Form a query context
		QueryResult res = IndexReader.getInstance().search(ctx); //Perform Search
		assertEquals(1, res.teasers.length); //Matching records count
		DocTeaser firstRecord = (DocTeaser) res.teasers[0]; //Get first record
		System.out.println(firstRecord.getId() + "\n" + firstRecord.title.toString());
	}
}
