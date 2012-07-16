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

import java.util.Date;

import com.bizosys.hsearch.PerformanceLogger;
import com.bizosys.hsearch.inpipe.InpipeLog;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.services.Service;
import com.bizosys.oneline.services.ServiceMetaData;
import com.bizosys.oneline.services.batch.BatchTask;
import com.bizosys.oneline.services.scheduler.ExpressionBuilder;
import com.bizosys.oneline.services.scheduler.ScheduleTask;

/**
 * This service refreshes the stopwords in intervals.
 * @author karan
 *
 */
public class HSearchHealthMonitor implements Service, BatchTask {
	
	ScheduleTask scheduledMonitor = null;
	public HSearchHealthMonitor() {
	}
	
	public String getName() {
		return "HSearchHealthMonitor";
	}

	/**
	 * Initialization.. Putting the recurring task
	 */
	public boolean init(Configuration conf, ServiceMetaData arg1) {
		InpipeLog.l.info("HSearchHealthMonitor > Initializing Service.");
		int monitorInteral = conf.getInt("monitor.interval", 1);
		if ( InpipeLog.l.isInfoEnabled()) InpipeLog.l.info(
			"HSearchHealthMonitor > refresh interal is " + monitorInteral);
		ExpressionBuilder expr = new ExpressionBuilder();
		expr.setSecond(0, false);
		expr.setMinute(monitorInteral, true);
		
		long startTime = new Date().getTime() + monitorInteral * 60 * 1000;
		try {
			this.process();
			InpipeLog.l.info("HSearchHealthMonitor Refresh Job :" + expr.getExpression());
			scheduledMonitor = new ScheduleTask(this, expr.getExpression(), 
				new Date(startTime), new Date(Long.MAX_VALUE));
			InpipeLog.l.info("HSearchHealthMonitor > Monitoring task is scheduled.");
			return true;
		} catch (Exception ex) {
			InpipeLog.l.fatal("HSearchHealthMonitor Initialization failed >", ex);
			return false;
		}
	}

	public void process(Request arg0, Response arg1) {
	}

	public void stop() {
		if ( null != this.scheduledMonitor) 
			this.scheduledMonitor.endDate = new Date(System.currentTimeMillis());
	}	

	/**
	 * Job Details
	 */
	public String getJobName() {
		return "hsearch-health";
	}

	public Object process() throws ApplicationFault, SystemFault {
		
		if ( ! PerformanceLogger.l.isInfoEnabled()) return null;
		
		Runtime runTime = Runtime.getRuntime();
		long maxMem = runTime.maxMemory()/1024;
		long totalMem = runTime.totalMemory()/1024;
		long freeMem = runTime.freeMemory()/1024;
		StringBuilder sb = new StringBuilder(96);
		sb.append("<m>");
		sb.append(maxMem).append('|').append(totalMem).append('|').append(freeMem);
		sb.append("</m>");
		
		
		PerformanceLogger.l.info(ObjectFactory.getInstance().getStatus());
		PerformanceLogger.l.info(sb.toString());
		
		return null;
	}

	public void setJobName(String arg0) {
	}
	
}
