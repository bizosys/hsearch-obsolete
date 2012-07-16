package com.bizosys.hsearch.validator;

public class NumberValidator {
    
    private NumberValidator() {
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
    public static boolean isInRange(long value, long min, long max) {
        return (value >= min && value <= max);
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
    public static boolean isInRange(Long value, long min, long max) {
        return isInRange(value.longValue(), min, max);
    }

    /**
     * Check if the value is greater than or equal to a minimum.
     * 
     * @param value The value validation is being performed on.
     * @param min The minimum value.
     * @return <code>true</code> if the value is greater than
     *         or equal to the minimum.
     */
    public static boolean minValue(long value, long min) {
        return (value >= min);
    }

    /**
     * Check if the value is greater than or equal to a minimum.
     * 
     * @param value The value validation is being performed on.
     * @param min The minimum value.
     * @return <code>true</code> if the value is greater than
     *         or equal to the minimum.
     */
    public static boolean minValue(Long value, long min) {
        return minValue(value.longValue(), min);
    }

    /**
     * Check if the value is less than or equal to a maximum.
     * 
     * @param value The value validation is being performed on.
     * @param max The maximum value.
     * @return <code>true</code> if the value is less than
     *         or equal to the maximum.
     */
    public static boolean maxValue(long value, long max) {
        return (value <= max);
    }

    /**
     * Check if the value is less than or equal to a maximum.
     * 
     * @param value The value validation is being performed on.
     * @param max The maximum value.
     * @return <code>true</code> if the value is less than
     *         or equal to the maximum.
     */
    public static boolean maxValue(Long value, long max) {
        return maxValue(value.longValue(), max);
    }
    
}
