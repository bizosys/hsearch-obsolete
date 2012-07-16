package com.bizosys.hsearch.loader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.validator.DateValidator;
import com.bizosys.hsearch.validator.DecimalValidator;
import com.bizosys.hsearch.validator.EMailValidator;
import com.bizosys.hsearch.validator.PhoneValidator;
import com.bizosys.hsearch.validator.StringValidator;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.util.StringUtils;

public class DataLoader {
	
    public static final int NONE = 0;
    public static final int NEUMERIC = 1;
    public static final int DECIMAL = 2;
    public static final int ALPHA = 3;
    public static final int ALPHANEUMERIC = 4;
    public static final int DATE = 5;
    public static final int EMAIL = 6;
    public static final int PHONE = 7;
    
    public static final String [] DATA_TYPE_NAMES = new String[]{
    	"NONE","NEUMERIC","DECIMAL","ALPHA",
    	"ALPHANEUMERIC", "DATE","EMAIL","PHONE"};    


    public static void load(URL url, boolean isHeader, 
		RowEventProcessor handler, String separator,
		int [] columnFormats, int [] nonEmptyCells,  
		int [] optionCheckCells, String optionCheckValues[],
		int [] minCheckCells, double[] minCheckValues,
		int [] maxCheckCells, double[] maxCheckValues) 
	throws ApplicationFault, SystemFault {
		
	    /**
	     * Populate cell choices
	     */
	    Map<Integer, String[]> optionCheck = new HashMap<Integer, String[]>();
	    for (int i=0; i< optionCheckValues.length; i++) {
	    	String[] choices = StringUtils.getStrings(optionCheckValues[i], ",");
	    	optionCheck.put(optionCheckCells[i], choices);
		}

	    /**
	     * Populate minimum values
	     */
	    Map<Integer, Double> minChecks = new HashMap<Integer, Double>();
	    
	    int minCheckCellsT = ( null == minCheckCells) ? 0 : minCheckCells.length;
	    for (int i=0; i<minCheckCellsT; i++) {
	    	minChecks.put(minCheckCells[i], minCheckValues[i]);
		}

	    /**
	     * Populate maximum values
	     */
	    Map<Integer, Double> maxChecks = new HashMap<Integer, Double>();
	    int maxCheckCellsT = ( null == maxCheckCells) ? 0 : maxCheckCells.length;
	    for (int i=0; i<maxCheckCellsT; i++) {
	    	maxChecks.put(maxCheckCells[i], maxCheckValues[i]);
		}
	    load(url, isHeader, handler, separator, columnFormats, 
	    	nonEmptyCells, optionCheck,minChecks,maxChecks); 
	}
    
    
    public static void load(URL url, boolean isHeader, 
    		RowEventProcessor handler,String separator, 
    		int [] columnFormats, int [] nonEmptyCells,  
    		Map<Integer, String[]> optionCheck,
    		Map<Integer, Double> minChecks,
    		Map<Integer, Double> maxChecks) 
    	throws ApplicationFault, SystemFault {
    		
    		InputStream  stream = null;
    		try {
    			stream = url.openStream();
    		} catch (IOException ex) {
    			throw new ApplicationFault(ex);
    		}
    		
    		//Don't buffer as CsvReader already does it
    		Reader isReader = new InputStreamReader(stream);
    		RowReader reader = RowReaderFactory.getReader(separator, isReader);
    	    
    	    String [] rowCells;
    	    
    	    int rowNumber = 0;
    	    try {
    		    while ((rowCells = reader.readNext()) != null) {
    		    	rowNumber++;
    	
    		        if ( isHeader ) {
    		    		handler.onHeaderRow(rowCells);
    		    		isHeader = false;
    		    		continue;
    		    	}
    	
    		        validateNonEmpty(rowCells, nonEmptyCells, rowNumber);
    		        validateFormats(columnFormats, rowCells, rowNumber);
    		        validateAllowedValues(rowCells, optionCheck, rowNumber);
    		        validateMinimumValues(rowCells, minChecks, rowNumber);
    		        validateMaximumValues(rowCells, maxChecks, rowNumber);
    		        handler.onDataRow(rowCells);
    		    }
    		    handler.onEnd();
    	    } catch (IOException ex) {
    	    	throw new SystemFault(ex);
    	    } finally {
    	    	if ( null != reader) try {reader.close();} 
    	    	catch (Exception ex) {LoaderLog.l.warn(ex);}
    	    }
    	}    
	
	private static void validateMaximumValues(String[] rowCells, Map<Integer, 
			Double> maxChecks, int rowNumber) throws ApplicationFault {
		
		if ( null == maxChecks) return;
		for (int cellIndex : maxChecks.keySet()) {
			String cell = rowCells[cellIndex];
			if ( StringUtils.isEmpty(cell)) continue;
			boolean isValid = DecimalValidator.maxValue(
				new Double(cell), maxChecks.get(cellIndex));
			if ( !isValid) throw new ApplicationFault(
				"Illegal Cell: [" + cell + "] at row=" + rowNumber + "\n" +
				StringUtils.arrayToString(rowCells, '|'));
			
		}
	}	

	private static void validateMinimumValues(String[] rowCells, Map<Integer, Double> minChecks, int rowNumber) throws ApplicationFault {
		if ( null == minChecks) return;
		for (int cellIndex : minChecks.keySet()) {
			String cell = rowCells[cellIndex];
			if ( StringUtils.isEmpty(cell)) continue;
			boolean isValid = DecimalValidator.minValue(
				new Double(cell), minChecks.get(cellIndex));
			if ( !isValid) throw new ApplicationFault(
				"Illegal Cell: [" + cell + "] at row=" + rowNumber + "\n" +
				StringUtils.arrayToString(rowCells, '|'));
			
		}
	}

	private static void validateAllowedValues(String[] rowCells, Map<Integer, String[]> cellChoices, int rowNumber) throws ApplicationFault {
		if ( null == cellChoices) return;
		boolean isValid = false;
		for (int cellNo : cellChoices.keySet()) {
			String cell = rowCells[cellNo]; 
			isValid = StringValidator.isValidSelect(cell, cellChoices.get(cellNo));
			if ( !isValid) throw new ApplicationFault(
				"Illegal Cell: [" + cell + "] at row=" + rowNumber + "\n" +
				StringUtils.arrayToString(rowCells, '|'));
		}
	}

	private static void validateNonEmpty(String[] rowCells, int[] nonEmptyCells, int rowNumber) throws ApplicationFault {
		 int cellsTotal = rowCells.length;

		 for (int cellIndex : nonEmptyCells) {
			if ( cellIndex >= cellsTotal) {
				throw new ApplicationFault("validateNonEmpty Failed : cellIndex/cellsTotal=" + 
					cellIndex + "/" + cellsTotal + "\n[[" + 
					StringUtils.arrayToString(rowCells, '\n') + "]]");
			}
			if ( StringUtils.isEmpty(rowCells[cellIndex]) )
				throw new ApplicationFault(
					"Illegal Empty Cell: [" + cellIndex + "] at row=" + rowNumber + "\n" +
					StringUtils.arrayToString(rowCells, '\n'));
		}
	}
	
	private static void validateFormats(int [] columnFormats, String [] rowCells, int rowNumber) throws ApplicationFault{
		 int columnFormatsT = columnFormats.length;
		 int cellsTotal = rowCells.length;
		 
		boolean isValid = false;
		int format = -1;
		String cell = null;

		if ( null != columnFormats) {
    		for (int i=0; i< columnFormatsT; i++) {
				if ( ! (i < cellsTotal) ) break; 
    			format = columnFormats[i];
    			cell = rowCells[i];
    			if ( StringUtils.isEmpty(cell)) continue;
    			
    			//if ( decode ) cell = StringUtils.decode(cell);
    			
    			isValid = false;
    			switch ( format ) {
		    		case NONE:
		    			isValid = true;
		    			break;

    		    	case NEUMERIC:
    		    		isValid = StringValidator.isValidNumberic(cell, false);
    		    		break;

    		    	case DECIMAL:
    		    		isValid = StringValidator.isValidDecimal(cell, false);
    		    		break;
    		    		
	    		    case ALPHA:
	    		    	isValid = StringValidator.isValidAlpha(cell, false);
    		    		break;
	    		    	
	    		    case ALPHANEUMERIC:
	    		    	isValid = StringValidator.isValidAlphaNumberic(cell, false);
    		    		break;
	    		    	
	    		    case DATE:
	    		    	isValid = DateValidator.isValid(cell, null);
    		    		break;
	    		    	
	    		    case EMAIL:
	    		    	isValid = EMailValidator.isValid(cell);
    		    		break;

	    		    case PHONE:
	    		    	isValid = PhoneValidator.isValid(cell);
    		    		break;

	    		    default:
	    		    	isValid = true;
	    		    	break;
    			}
    			
    			if ( !isValid) throw new ApplicationFault(
    				"Illegal Cell: [" + cell + "] for format " + DATA_TYPE_NAMES[format] + " at row=" + rowNumber + "\n" +
    				StringUtils.arrayToString(rowCells, '|'));
			}
    	}		
	}

}
