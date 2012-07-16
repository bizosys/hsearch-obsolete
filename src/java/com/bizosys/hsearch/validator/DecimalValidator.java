package com.bizosys.hsearch.validator;

import com.bizosys.oneline.util.StringUtils;

public class DecimalValidator {
    
    private DecimalValidator() {
    }
    
    /**
     * Check if the value is within a specified range.
     * 
     * @param value The <code>Number</code> value to check.
     * @param min The minimum value of the range.
     * @param max The maximum value of the range.
     * @return <code>true</code> if the value is within the
     *         specified range.
     */
    public static boolean isInRange(float value, float min, float max) {
        return (value >= min && value <= max);
    }

    public static boolean isInRange(double value, double min, double max) {
        return (value >= min && value <= max);
    }

    /**
     * Check if the value is greater than or equal to a minimum.
     * 
     * @param value The value validation is being performed on.
     * @param min The minimum value.
     * @return <code>true</code> if the value is greater than
     *         or equal to the minimum.
     */
    public static boolean minValue(float value, float min) {
        return (value >= min);
    }

    public static boolean minValue(double value, double min) {
        return (value >= min);
    }

    /**
     * Check if the value is less than or equal to a maximum.
     * 
     * @param value The value validation is being performed on.
     * @param max The maximum value.
     * @return <code>true</code> if the value is less than
     *         or equal to the maximum.
     */
    public static boolean maxValue(float value, float max) {
        return (value <= max);
    }
    
    public static boolean maxValue(double value, double max) {
        return (value <= max);
    }

    public static boolean isValid(String val, String constraint ) {
    	if ( null == val ) return false;
    	try {
    		float typeVal = Float.parseFloat(val);
   			String[] minMax = StringUtils.getStrings(constraint, StringUtils.SEPARATOR_RECORD);
   			boolean hasMin = ! StringUtils.isEmpty(minMax[0]);
   			boolean hasMax = ! StringUtils.isEmpty(minMax[1]);
   			
   			if ( hasMin ) {
   				float minVal = Float.parseFloat(minMax[0]);
   				if ( hasMax ) return isInRange(typeVal, minVal, Float.parseFloat(minMax[1]));
   				else return minValue(typeVal, minVal);
   			} else {
   				if ( hasMax ) return maxValue(typeVal, Float.parseFloat(minMax[1]));
   				else return true;
   			}
    	} catch (Exception ex) {
    		return false;
    	}
    }
    
    public static boolean isValid(String val) {
    	try {
    		Float.parseFloat(val);
   			return true;
    	} catch (Exception ex) {
    		return false;
    	}
    }
}
