/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * Class to handle HTTP dates.
 * Modified from FastHttpDateFormat.java in jakarta-tomcat.
 * @author John Xing
 */
public class HttpDateFormat {

  protected static SimpleDateFormat[] formats = new SimpleDateFormat[] { 
    new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", new Locale("en","IN")),
    new SimpleDateFormat("EEE MMMM d hh:mm:ss yyyy", new Locale("en","IN")),
    new SimpleDateFormat("EEEEEE, dd-MMM-yy HH:mm:ss zzz", new Locale("en","IN")),
    new SimpleDateFormat("EEEEEE, dd MMMM, yyyy HH:mm:ssaa", new Locale("en","IN"))
        
  };

  /**
   * HTTP date uses TimeZone GMT
   */
  static {
	  for (SimpleDateFormat format : formats) {
		  format.setTimeZone(TimeZone.getTimeZone("IST"));
	  }
  }

  /**
   * Get the HTTP format of the specified date.
   */
  public static String toString(Date date) {
    String string;
    synchronized (formats[0]) {
      string = formats[0].format(date);
    }
    return string;
  }

  public static String toString(Calendar cal) {
    String string;
    synchronized (formats[0]) {
      string = formats[0].format(cal.getTime());
    }
    return string;
  }

  public static String toString(long time) {
    String string;
    synchronized (formats[0]) {
      string = formats[0].format(new Date(time));
    }
    return string;
  }

  public static Date toDate(String dateString) throws ParseException {
	  Date date = null;
	  char thirdChar = dateString.charAt(3);
	  if ( ',' == thirdChar ) {
	        synchronized (formats[0]) { date = formats[0].parse(dateString); }
	        return date;
	  } else if ( ' ' == thirdChar  ) {
	        synchronized (formats[1]) { date = formats[1].parse(dateString); }
	        return date;
	  } else  if ( dateString.endsWith("am") || dateString.endsWith("pm")) {
	        synchronized (formats[3]) { date = formats[3].parse(dateString); }
	        return date;
	  } else {
	        synchronized (formats[2]) { date = formats[2].parse(dateString); }
	        return date;
	  }	  
  }

  public static long toLong(String dateString) throws ParseException {
	  Date date = toDate(dateString);
	  if ( null != date) return date.getTime();
	  else return -1;
  }
}
