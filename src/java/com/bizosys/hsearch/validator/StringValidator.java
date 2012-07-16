package com.bizosys.hsearch.validator;

import com.bizosys.oneline.util.StringUtils;

public class StringValidator {
	
	/**
	 * Validates a string for characters from a-z or A-Z
	 * @param line
	 * @return
	 */
	public static boolean isValidAlpha(String line, boolean noBlank) {
		
		boolean isValid = false;
		if ( null == line ) return false;
		char[] arr = line.toCharArray();
		for ( char c : arr ) {
			isValid = (c >= 'A' && c <= 'z') || (c == ' ') ;
			if ( ! isValid ) return false;
		}

		if ( noBlank ) return line.length() > 0 ;
		return true;
	}
	
	public static boolean isValidAlphaNumberic(String line, boolean noBlank) {
		
		if ( null == line ) return false;
		char[] arr = line.toCharArray();
		boolean validAlpha = false;
		boolean validNo = false;
		for ( char c : arr ) {
			validAlpha = (c >= 'A' && c <= 'z') || (c == ' ') ;
			validNo = c >= '0' && c <= '9';
			if ( ! (validAlpha || validNo) ) {
				return false;
			}
		}
		
		if ( noBlank ) return line.length() > 0 ;
		return true;
	}
	
	public static boolean isValidNumberic(String line, boolean noBlank) {
		
		if ( null == line ) return false;
		char[] arr = line.toCharArray();
		boolean validNo = false;
		for ( char c : arr ) {
			validNo = c >= '0' && c <= '9';
			if ( ! validNo ) return false;
		}
		
		if ( noBlank ) return line.length() > 0 ;
		return true;
	}
	
	public static boolean isValidDecimal(String line, boolean noBlank) {
		
		if ( null == line ) return false;
		char[] arr = line.toCharArray();
		boolean validDecimal = false;
		boolean validNo = false;
		for ( char c : arr ) {
			validDecimal = (c == '.' || c == '-' || c == '+') ;
			validNo = c >= '0' && c <= '9';
			if ( ! (validDecimal || validNo) ) return false;
		}
		
		if ( noBlank ) return line.length() > 0 ;
		return true;
	}	

	public static boolean isValidSelect(String line, String choices) {
		
		if ( StringUtils.isEmpty(line) ) return false;
		String[] choicesL = 
			StringUtils.getStrings(choices, StringUtils.SEPARATOR_RECORD_STR);
		if ( null == choicesL ) return false;
		return isValidSelect(line, choicesL);
	}
	
	public static boolean isValidSelect(String value, String[] choicesList) {
		
		boolean isMatched = false;
		for ( String aChoice : choicesList) {
			isMatched = value.equals(aChoice);
			if ( isMatched ) break;
		}
		return isMatched; 
	}	
}