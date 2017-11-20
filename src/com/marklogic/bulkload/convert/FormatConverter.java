package com.marklogic.bulkload.convert;

import java.util.List;

import org.codehaus.jettison.json.JSONObject;

import com.marklogic.bulkload.Logger;
import com.marklogic.bulkload.write.Item;

/**
 * Convert a set of related lines to one record.
 * 
 * @author Damon
 *
 */
public abstract class FormatConverter<T> {
    Logger logger = new Logger(FormatConverter.class); // Logger.getLogger(this.getClass().getName());

	protected static final char DELIMITER = '\t';
	protected String[] headerArray;
	protected int idColumnIndex;
	protected JSONObject summaryRecord;
	protected final SummaryInfo summaryInfo;	
	
	// TODO: push this down to DocumentFormatConverter. Most of this is not needed for simple unparsed line processing
	public FormatConverter( String[] headerArray,
						    int idColumnIndex,
						    SummaryInfo summaryInfo) {
		this.summaryInfo = summaryInfo;
		
		this.headerArray = headerArray;
		this.idColumnIndex = idColumnIndex;
		this.summaryRecord = summaryInfo.toJson();
		
    }
	
    public abstract T convert(List<String> lines) ;


}