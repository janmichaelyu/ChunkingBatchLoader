package com.marklogic.bulkload.write;

import java.util.Collection;
import java.util.List;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;

/** 
 * Writes a chunk of documents by creating a single XCC contentInsert call with all of them.
 * 
 * @author Damon
 *
 */
public class XccRawLinesInJSONStructureChunkWriter extends XccChunkWriter<List<String>> {

	private static final String BATCH_HUB_INPUT_XQY = "/batchSendLinesToHubFlow.xqy";
	
	private Collection summaryRecord;

	public XccRawLinesInJSONStructureChunkWriter(List<ContentSource> contentSources) {
		super(contentSources);
	}

	protected void doWrite(List<List<String>> items, ContentSource contentSource) {
		int numRecords = items.size();
		for (int i=0; i<numRecords; i++) {
			List<String> itemStrings = items.get(i);

	    	JSONObject allRecords;
	    	try {
		    	allRecords = new JSONObject();
		    	allRecords.put("headers", summaryRecord);
		    	
		    	JSONArray dataArray = new JSONArray(itemStrings.size());
		    	for (String line : itemStrings) { 
		            dataArray.put(line);
		    	}
		    	
		    	allRecords.put("data", dataArray);
	    	} catch (JSONException ex) {
	    		throw new RuntimeException("Could not serialize JSON", ex);
	    	}

			// Write the data!
	    	String all = allRecords.toString();
			try {
				Session sess = contentSource.newSession();
				Request r = sess.newModuleInvoke(BATCH_HUB_INPUT_XQY); 
				r.setNewStringVariable("allItems", all);
				sess.submitRequest(r);
			} catch (Exception ex) {
				throw new RuntimeException("Unable to insert content set ", ex);
			}
		}

	}

}
