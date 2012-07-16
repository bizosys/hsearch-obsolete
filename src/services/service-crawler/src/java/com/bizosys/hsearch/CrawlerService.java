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

import org.apache.log4j.Logger;

import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.util.StringUtils;

public class CrawlerService implements Service {

	public static Logger l = Logger.getLogger(CrawlerService.class.getName());
	
	Configuration conf = null;
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		this.conf = conf;
		l.info("Initializing Tab Fetcher.");
		return true;
	}

	public void stop() {
	}
	
	public String getName() {
		return "TabfetcherService";
	}	
	
	public void process(Request req, Response res) {
		
		String action = req.action; 

		try {

			if ( "tabfile".equals(action) ) {
				this.indexTabfile(req, res);
			} else {
				res.error("Failed Unknown operation : " + action);
			}
		} catch (Exception ix) {
			l.fatal("SearchService > ", ix);
			res.error("Failure : SearchService:" + action + " " + ix.getMessage());
		}
	}
	
	/**
	 * Gets a document given the {id}
	 * @param req
	 * @param res
	 * @throws ApplicationFault
	 * @throws SystemFault
	 */
	private void indexTabfile(Request req, Response res) throws ApplicationFault, SystemFault{
		String file = req.getString("file", true, true, false);
		Object pristineO = req.getObject("hdoc", false);
		HDocument hdoc = ( null == pristineO) ? 
			new HDocument() : (HDocument) pristineO;
			
		String idFldName = req.getString("idFldName", true, true, false);
		String titleFldNames = req.getString("titleFldNames", true, true, false);
		String previewFields = req.getString("previewFields", true, true, false);
		String runPlan = req.getString("runPlan", true, true, false);
		int startIndex = req.getInteger("startIndex", true);
		int endIndex = req.getInteger("endIndex", true);
		int batchSize = req.getInteger("batchSize", true);
		
		TabFileCrawler crawler = new TabFileCrawler(
			"anonymous",	
			file,hdoc,idFldName,  
			StringUtils.getStrings(titleFldNames, ","),
			StringUtils.getStrings(previewFields, ","),
			IndexWriter.getInstance().getPipes(runPlan),
			startIndex, endIndex, batchSize); 
		crawler.fetchAndIndex();
	}
}
