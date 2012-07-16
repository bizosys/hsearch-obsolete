package com.bizosys.hsearch;

import org.apache.log4j.Logger;

import com.bizosys.oneline.conf.Configuration;
import com.bizosys.oneline.services.Request;
import com.bizosys.oneline.services.Response;
import com.bizosys.oneline.session.Session;

public class Perf implements Session { 
	public static String clazz = Perf.class.getName();
	private static Logger LOG = PerformanceLogger.l;
	private static int activeSessions = 0;
	
	
	public String getClazz() {
		return Perf.clazz;
	}

	public void setConfig(Configuration config) {
		
	}
	
	public Object onStart(Request context, Response response) {
		activeSessions++;
		Long startTime  = new Long(System.currentTimeMillis());
		return startTime;
	}

	public void onFinish(Object startInfo, Request request, Response response) {
		
		long endTime  = System.currentTimeMillis();
		long startTime = ((Long) startInfo).longValue();
		long diff = endTime - startTime;
		Runtime runTime = Runtime.getRuntime();
		long maxMem = runTime.maxMemory()/1024;
		long totalMem = runTime.totalMemory()/1024;
		long freeMem = runTime.freeMemory()/1024;
		
		if ( LOG.isInfoEnabled() ) {
			StringBuffer strBuf = new StringBuffer("<p ");
			String user = ( null == request.user ) ? "xuser" : request.user.toString();   
			strBuf.append("user=\"").append(user).append("\" ");
			strBuf.append("sensor=\"").append(request.serviceId).append("\" ");
			strBuf.append("time=\"").append(diff).append("\" ");
			strBuf.append("sessions=\"").append(activeSessions).append("\" ");
			strBuf.append("memory(MTF)=\"").append(maxMem).append(',').
			append(totalMem).append(',').append(freeMem).append("\" />");
			LOG.info(strBuf.toString());
		}
		activeSessions--;
	}
}
