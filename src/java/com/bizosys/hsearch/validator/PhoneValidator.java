package com.bizosys.hsearch.validator;

public class PhoneValidator {
	
	public static boolean isValid(String line) {
		
		if ( null == line ) return false;
		char[] arr = line.trim().toCharArray();
		for ( char c : arr ) {
			boolean validChar = c == '1' || c == '2' || c == '3' || c == '4' ||
				 c == '5' || c == '6' || c == '7' || c == '8' || 
				 c =='9' || c == '0' || c == '+' || c == '-' || 
				 c == '(' || c == ')' ;
			if ( ! validChar ) return false;
		}
		return true;
	}
}
