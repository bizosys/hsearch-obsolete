package com.bizosys.hsearch.loader;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.common.Field;
import com.bizosys.hsearch.common.HDocument;
import com.bizosys.hsearch.common.SField;
import com.bizosys.hsearch.common.Account.AccountInfo;
import com.bizosys.hsearch.index.IndexWriter;
import com.bizosys.hsearch.loader.csv.CsvWriter;
import com.bizosys.oneline.ApplicationFault;
import com.bizosys.oneline.SystemFault;
import com.bizosys.oneline.pipes.PipeIn;
import com.bizosys.oneline.util.StringUtils;

public class RowEventProcessorHSearch implements RowEventProcessor {
	
	/**
	 * About the User
	 */
	private AccountInfo acc = null;

	/**
	 * About the document
	 */
	private String[] headings = null;
	private String idPrefix= null; 
	private int idFldColumn= -1; 
	private int urlFldColumn= -1; 
	private int weightFldColumn = -1;
	private int idAutoIncrement = 1;
	private int[] titleColumns = null;
	private int keywordColumn = -1;
	private int[] previewColumns = null;
	private int[] descFldColumns = null;
	private int[] indexableColumns = null;
	private String documentType = null;

	/**
	 * About Indexing with other meta info
	 */
	private HDocument pristineDoc = null;
	private List<PipeIn> runPlan = null;
	private boolean isXmlPreview = true;
	
	/**
	 * Prebuilt variables
	 */
	private String recordStartTag = null; 
	private String recordEndTag = null;
	private int indexableColsTotal = 0;
	private int titleColsTotal = 0;
	private int previewColsTotal = 0;
	private int descColsTotal = 0;
	private CsvWriter csvWriter = new CsvWriter(',');
	
	private StringBuilder descriptionBuilder = new StringBuilder(512);
	
	
	/**
	 * Indexing Cursor Details
	 */
	private int startIndex = 0;
	private int endIndex = -1;
	boolean isEndIndex = false;
	int readDocs = 0;
	
	private List<String[]> rows = new ArrayList<String[]>();
	private List<HDocument> hdocs = new ArrayList<HDocument>();
	private int betchSize = 300;
	private Writer writer = null;	
	
	private String lineBreak = null;
	
	public RowEventProcessorHSearch(
		AccountInfo acc, 
		HDocument pristineDoc, List<PipeIn> plan,
		String idPrefix, int idFldColumn, int urlFldColumn, 
		int weightFldColumn, int[] titleFldColumns, int keywordColumn,
		int[] previewFldColumns, int[] descFldColumns,
		String documentType, int[] indexFldColumns,
		int startIndex, int endIndex, int batchSize, boolean isXmlPreview,
		Writer writer, String lineBreak)
		throws ApplicationFault, SystemFault {
		
		
		this.acc = acc;
		this.pristineDoc = pristineDoc;
		this.runPlan = ( null == plan) ? 
			IndexWriter.getInstance().getInsertPipes() : plan;
    	
		this.idPrefix = idPrefix;
		if ( StringUtils.isEmpty(this.idPrefix)) 
			this.idPrefix = StringUtils.Empty;
		this.idFldColumn = idFldColumn;
		this.urlFldColumn = urlFldColumn;
		this.weightFldColumn = weightFldColumn;
		
		this.titleColumns = titleFldColumns;
		this.titleColsTotal = ( null == titleFldColumns) ? 0 : titleFldColumns.length;
		
		this.keywordColumn = keywordColumn;
		
		this.previewColumns = previewFldColumns;
		previewColsTotal = ( null == previewColumns) ? 0 : previewColumns.length;
		
		this.descFldColumns = descFldColumns;
		descColsTotal = ( null == descFldColumns) ? 0 : descFldColumns.length; 
		
		this.indexableColumns = indexFldColumns;
		this.indexableColsTotal = ( null == indexFldColumns ) ? 0 : indexFldColumns.length;
		if ( LoaderLog.l.isDebugEnabled() ) LoaderLog.l.debug(
			"Total Indexable Columns: " + this.indexableColsTotal);
	    
		this.documentType = documentType;
		this.recordStartTag = "<" + this.documentType + ">"; 
		this.recordEndTag= "</" + this.documentType + ">";
	    	
		if ( endIndex  != -1) {
			if ( startIndex >= endIndex ) throw new ApplicationFault(
				"Not allowed as reading ends at " + endIndex);
		}

		this.startIndex = startIndex;
		if ( endIndex != -1 && endIndex <= startIndex ) 
			throw new ApplicationFault("Not allowed as reading starts from " + startIndex);

		this.endIndex = endIndex;
		if ( this.endIndex > 0 ) isEndIndex = true;
		
		this.betchSize = batchSize;
		this.isXmlPreview = isXmlPreview;
		
		this.writer = writer;
		if ( null == writer) {
			try {
				this.writer = new OutputStreamWriter(System.out, "UTF-8");
			} catch (UnsupportedEncodingException ex) {
				//This will never happen
				LoaderLog.l.warn( "OutputStream encoding issues", ex);
			}
		}

		this.lineBreak = lineBreak;
	}

	public void onHeaderRow(String[] cells) throws ApplicationFault, SystemFault {
		int totalCells = ( null == cells ) ? 0 : cells.length; 
		if ( 0 == totalCells) throw new ApplicationFault("There is no header row");
		for (String cell : cells) {
			if ( cell.length() > 24 ) {
				throw new ApplicationFault("In Appropriate Header.. More than 16 character.\n" + StringUtils.arrayToString(cells, '|'));
			}
		}

		this.readDocs++;
		this.headings = cells;
	}

	public void onDataRow(String[] cells) throws ApplicationFault, SystemFault {
		this.readDocs++;
		if ( this.readDocs < this.startIndex) return; //Not reached yet
		if ( isEndIndex ) if ( this.readDocs > this.endIndex) return; // Already done
		
		this.rows.add(cells);
		if ( this.rows.size() >= this.betchSize) {
			insert(this.rows, this.runPlan);
			this.rows.clear();
		}
	}
	
	public void onEnd() throws ApplicationFault, SystemFault  {
		//Flush the rest
		if ( this.rows.size() > 0 ) {
			insert(this.rows, this.runPlan);
			this.rows.clear();
		}
	}	

	/**
	 * Insert
	 * @param records
	 * @param runPlan
	 * @return
	 * @throws Exception
	 */
	private void insert(List<String[]> rows, List<PipeIn> runPlan ) 
	throws ApplicationFault, SystemFault {
		
		StringBuilder text = new StringBuilder(1024);
		StringBuilder title = new StringBuilder();
		StringBuilder preview = new StringBuilder(1024);
		
		hdocs.clear();
		for (String[] cells : rows) {
			
			text.delete(0, text.capacity());
			title.delete(0, title.capacity());
			preview.delete(0, preview.capacity());
			
			HDocument aDoc = new HDocument();
			aDoc.docType = this.pristineDoc.docType;
			aDoc.url = this.pristineDoc.url;
			aDoc.eastering = this.pristineDoc.eastering;
			aDoc.team = this.pristineDoc.team;
			aDoc.editPermission = this.pristineDoc.editPermission;
			aDoc.ipAddress = this.pristineDoc.ipAddress;
			aDoc.locale = this.pristineDoc.locale;
			aDoc.northing = this.pristineDoc.northing;
			aDoc.securityHigh= this.pristineDoc.securityHigh;
			aDoc.sentimentPositive = this.pristineDoc.sentimentPositive;
			aDoc.viewPermission = this.pristineDoc.viewPermission;
			
			if ( -1 == this.idFldColumn) {
				if ( null == this.idPrefix) aDoc.key = new Integer(this.idAutoIncrement++).toString();
				else aDoc.key = this.idPrefix + (this.idAutoIncrement++);
				
			} else {
				if ( StringUtils.isEmpty(cells[this.idFldColumn]) ) {
					String msg = "\nEmpty Id.. Skipping > " + StringUtils.arrayToString(cells, '|');
					try {
						this.writer.write(msg);
					} catch (IOException ex) {
						throw new ApplicationFault(ex);
					}
					continue;
				}
				aDoc.key = this.idPrefix + cells[this.idFldColumn];
			}
			
			if ( -1 != this.urlFldColumn) {
				if ( ! StringUtils.isEmpty(cells[this.urlFldColumn]) ) {
					aDoc.url = cells[this.urlFldColumn];
				}
			}
			
			if ( ! StringUtils.isEmpty(cells[this.weightFldColumn]) ) {
				aDoc.weight = new Integer(cells[this.weightFldColumn]).intValue();
			}

			
			if ( this.keywordColumn > -1) {
				if ( this.keywordColumn >= cells.length ) {
					throw new ApplicationFault(
						"Keyword column does not exist : " + 
						cells.length + "/" + this.keywordColumn);
				}
				String keywords = cells[this.keywordColumn];
				
				aDoc.tags = tagSplit(keywords, " , ");
				
				if ( LoaderLog.l.isDebugEnabled()) {
					LoaderLog.l.debug("Keyword:" + aDoc.tags.toString());
				}
			}
			
			if ( indexableColsTotal > 0) {
				aDoc.fields = new ArrayList<Field>(indexableColsTotal);
				for (int columnNumber : indexableColumns) {
					if ( StringUtils.isEmpty(cells[columnNumber]) ) continue;
					aDoc.fields.add(new SField(headings[columnNumber], cells[columnNumber]) );
				}
			}
			
			if ( titleColsTotal > 0) {
				if ( titleColsTotal > 1 ) aDoc.title = buildTitle(title, cells);
				else aDoc.title = cells[this.titleColumns[0]];
			}
				
			if ( previewColsTotal > 0) {
				if ( previewColsTotal > 1 ) aDoc.preview = this.buildPreview(preview, cells);
				else aDoc.preview = cells[this.previewColumns[0]];
				
				if ( null != this.lineBreak) {
					if ( aDoc.preview.indexOf(this.lineBreak) >= 0) {
						aDoc.preview = aDoc.preview.replace(this.lineBreak, "\n");
					}
				}
			}
			
			if ( descColsTotal > 0) {
				if ( descColsTotal > 1 ) aDoc.cacheText = this.buildDescription(cells);
				else aDoc.cacheText = cells[this.descFldColumns[0]];
				
				if ( null != this.lineBreak) {
					if ( aDoc.cacheText.indexOf(this.lineBreak) >= 0) {
						aDoc.cacheText = aDoc.cacheText.replace(this.lineBreak, "\n");
					}
				}
			}
			
			hdocs.add(aDoc);			
		}

		long s = System.currentTimeMillis();
		try {
			if ( LoaderLog.l.isInfoEnabled() ) {
				LoaderLog.l.info("\nInserting> End Row Id :" + readDocs + ", Batch Size:" + hdocs.size());
			}
			this.writer.flush();
			IndexWriter.getInstance().insertBatch(hdocs, this.acc, runPlan, true);
			if ( LoaderLog.l.isInfoEnabled() ) {
				long e = System.currentTimeMillis();
				LoaderLog.l.info("\nInsered> " + (e -s) + "ms");
			}
		} catch (Exception ex) {
			String msg = "Failed where> End Row Id :" + readDocs + ", Batch Size:" + hdocs.size();
			try { this.writer.write(msg); } catch (Exception iex){};
			LoaderLog.l.fatal(ex);
			throw new ApplicationFault(ex);
		}
	}
	

	/**
	 * Populate the title
	 * @param title
	 * @param cols
	 * @return
	 */
	private String buildTitle(StringBuilder title, String[] cells) {
		String colVal = null;
		if ( 1 == this.titleColsTotal) {
			return cells[this.titleColumns[0]];
		} 
		
		for (int aTitleCol : this.titleColumns) {
			colVal = cells[aTitleCol];
			if ( StringUtils.isEmpty(colVal) ) continue;
			title.append(colVal);
			title.append(", ");
		}
		return title.toString().trim();
	}

	/**
	 * Build the preview.
	 * @param xml
	 * @param cols
	 * @return
	 */
	private String buildPreview( StringBuilder preview, String[] cells) {
		
		String cellVal = null;		
		String colName = null;
		
		if ( isXmlPreview ) {
			preview.append(this.recordStartTag);
			for (int colIndex : this.previewColumns) {
				cellVal = cells[colIndex];
				if ( StringUtils.isEmpty( cellVal) ) continue;
				colName = headings[colIndex];
				
				preview.append('<').append(colName).append('>');
				preview.append(cells[colIndex]);
				preview.append("</").append(colName).append('>');
			}
			preview.append(this.recordEndTag);
			return preview.toString();
		} else {
			return csvWriter.writeRow(cells, preview);
		}
	}
	
	/**
	 * Build the description.
	 * @param xml
	 * @param cols
	 * @return
	 */
	private String buildDescription( String[] cells) {
		
		String cellVal = null;
		for (int colIndex : this.descFldColumns) {
			cellVal = cells[colIndex];
			if ( StringUtils.isEmpty( cellVal) ) continue;
			descriptionBuilder.append(cellVal).append(' ');
		}
		String res = descriptionBuilder.toString();
		descriptionBuilder.delete(0, descriptionBuilder.capacity());
		return res;
	}	
	
	  private static List<String> tagSplit(final String text, String separator) {
		  if ( null == text)return null;
		  if (text.length() == 0 ) return null;
		  int separatorLen = separator.length();

		  final List<String> result = new ArrayList<String>();
		  int index1 = 0;
		  int index2 = text.indexOf(separator);
		  String token = null;
		  while (index2 >= 0) {
			  token = text.substring(index1, index2);
			  result.add(token);
			  index1 = index2 + separatorLen;
			  index2 = text.indexOf(separator, index1);
		  }
	            
		  if (index1 < text.length() - 1) {
			  result.add(text.substring(index1));
		  }
		  return result;
	  }

}
