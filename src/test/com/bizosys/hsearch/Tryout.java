package com.bizosys.hsearch;

import java.lang.management.*;

public class Tryout {

	public static void main(String [] args) throws Exception
    {
		//String a = "Abinash Abinash Karan";
		//a = a.replace("Abinash", "Kaka");
	    OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean( );
	    long a1 = getCpuTime();
	    for ( long i=0; i<100000L; i++) {}
	    //Thread.sleep(60000);
	    long a2 = getCpuTime();
	    
		System.out.println("Remaining " + (new Long(  (a2 - a1)/1000000000 )).toString());
		System.out.println("Load : " + new Double(bean.getSystemLoadAverage()).toString());
   }
	
	/** Get CPU time in nanoseconds. */
	public static long getCpuTime( ) {
	    ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
	    return bean.isCurrentThreadCpuTimeSupported( ) ?
	        bean.getCurrentThreadCpuTime( ) : 0L;
	}
	 
	/** Get user time in nanoseconds. */
	public static long getUserTime( ) {
	    ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
	    return bean.isCurrentThreadCpuTimeSupported( ) ?
	        bean.getCurrentThreadUserTime( ) : 0L;
	}

	/** Get system time in nanoseconds. */
	public static long getSystemTime( ) {
	    ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
	    return bean.isCurrentThreadCpuTimeSupported( ) ?
	        (bean.getCurrentThreadCpuTime( ) - bean.getCurrentThreadUserTime( )) : 0L;
	}	
 
}