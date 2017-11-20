package com.marklogic.bulkload.convert;

import java.util.List;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.marklogic.bulkload.Logger;

/**
 * Created by Damon on 11/19/17.
 */
public class UnparsedLinesWriteConverter extends FormatConverter<List<String>> {
    
	private Logger logger = new Logger(UnparsedLinesWriteConverter.class); 
    private JSONObject summaryRecord;
    private Set<String> columnsOfInterest;
    
    public UnparsedLinesWriteConverter(
							String[] headerArray,
							int idColumnIndex,
							Set<String> columnsOfInterest,
							SummaryInfo summaryInfo) {
    	super(headerArray, idColumnIndex, summaryInfo);
        this.summaryRecord = summaryInfo.toJson();
        this.columnsOfInterest = columnsOfInterest;
    }

    /**
     * Bundle up the raw strings together with some metadata. Encode it as JSON (but leave the strings in raw form).
     * The data hub framework will do the bulk of the work parsing the strings and formatting as JSON.*/
    public List<String> convert(List<String> lines) {
    	return lines;
    }
    
    /****** 
     * 
     * TODO: move this logic to the XccHubDelegatingChunkWriter
     *
    	JSONObject allRecords;
    	try {
	    	allRecords = new JSONObject();
	    	allRecords.put("headers", summaryRecord);
	    	
	    	JSONArray contents = new JSONArray(lines.size());
	    	for (String line : lines) { 
	            contents.put(line);
	    	}
	    	
	    	allRecords.put("contents", contents);
    	} catch (JSONException ex) {
    		throw new RuntimeException("Could not serialize JSON", ex);
    	}
    	return allRecords;
    }
    */

}