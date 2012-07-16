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

import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.inpipe.InpipeLog;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;

/**
 * Initializes both the read and write index services.
 * @author karan
 *
 */
public class IndexService implements Service {
	
	private static IndexService instance = null;
	
	public static IndexService getInstance() {
		if ( null != instance) return instance;
		synchronized (IndexService.class) {
			if ( null != instance) return instance;
			instance = new IndexService();
		}
		return instance;
	}
	
	Map<String, PipeIn> pipes = new HashMap<String, PipeIn>(); 
	
	public IndexService(){
	}
	
	public boolean init(Configuration conf, ServiceMetaData meta) {
		try {
			InpipeLog.l.info("Initializing Index Service:");
			IndexWriter.getInstance().init(conf);
			IndexReader.getInstance().init(conf);
			return true;
		} catch (Exception e) {
			InpipeLog.l.fatal("Pipe Initialization Failure :" , e );
			return false;
		}		
	}

	public void process(Request req, Response res) {
		
	}

	public String getName() {
		return "IndexService";
	}

	public void stop() {
	}
	
}
