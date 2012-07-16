package com.bizosys.hsearch.validator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.bizosys.oneline.util.StringUtils;

public class DateValidator {

	private DateValidator() {
	}
	
    public static boolean isInRange(Date value, Date past, Date future) {
    	return pastValue(value,past ) && futrureValue(value, future); 
    }

    public static boolean pastValue(Date value, Date past) {
    	past.setTime(past.getTime() - 1); 
    	return value.after(past);
    }
    
    public static boolean futrureValue(Date value, Date future) {
    	future.setTime(future.getTime() + 1); 
    	return value.before(future);
    }
    
    public static boolean isValid(String val, String constraint ) {
    	if ( null == val ) return false; 
    	try {
    		DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH.mm.ss zzz");
    		df.setLenient(true);   // this is important!
    		Date typeVal = df.parse(val);

    		if ( StringUtils.isEmpty(constraint)) return true;
   			String[] pastFuture = StringUtils.getStrings(constraint, StringUtils.SEPARATOR_RECORD);
   			boolean hasPast = ! StringUtils.isEmpty(pastFuture[0]);
   			boolean hasFuture = ! StringUtils.isEmpty(pastFuture[1]);
   			
   			if ( hasPast ) {
   				Date pastVal = df.parse(pastFuture[0]);
   				if ( hasFuture ) return isInRange(typeVal, pastVal, df.parse(pastFuture[1]));
   				else return pastValue(typeVal, pastVal);
   			} else {
   				if ( hasFuture ) return futrureValue(typeVal, df.parse(pastFuture[1]));
   				else return true;
   			}
    		
    	} catch (ParseException e) {
    		return false;
    	} catch (IllegalArgumentException e) {
    		return false;
    	}
    }
 
}
