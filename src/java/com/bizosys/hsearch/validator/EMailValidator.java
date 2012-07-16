package com.bizosys.hsearch.validator;

import com.bizosys.oneline.util.StringUtils;

public class EMailValidator {

    private EMailValidator() {
    }
    
    public static boolean isValid(String emailId) {
    	
    	//Name and domain existance
        String[] nameDomain = StringUtils.getStrings(emailId,  '@');
        if ( null == nameDomain[0] || null == nameDomain[1] ) return false;
        String name = nameDomain[0].trim();
        String domain = nameDomain[1].trim();
        if ( name.length() < 2 || domain.length() < 7 || 
        	domain.indexOf('.') == -1 ) return false;

        return true;
    }
}
