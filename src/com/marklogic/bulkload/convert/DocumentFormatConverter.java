package com.marklogic.bulkload.convert;

import com.marklogic.bulkload.Logger;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by jmichael on 11/9/17.
 */
public class DocumentFormatConverter extends FormatConverter<DocumentWriteOperation> {

    Logger logger = new Logger(DocumentFormatConverter.class); 
    
	protected Set<String> columnsOfInterest;
	private List<String> dateColumns;
	
    public DocumentFormatConverter(String[] headerArray,
                                  int idColumnIndex,
                                  Set<String> columnsOfInterest,
                                  SummaryInfo summaryInfo) {
    	super(headerArray, idColumnIndex, summaryInfo);

    	this.columnsOfInterest = columnsOfInterest;
		dateColumns = new ArrayList<String>();
		for (String column : columnsOfInterest){
			if (column.endsWith("DATE")) {
				dateColumns.add(column);
			}
		}
    }


	/**
	 “TruvenEnrollment”: [
	 { “SEQNUM” : “65320181806” ,
	 instance: {
	 "DISDATE_CONVERTED": "2016-10-23",
	 },
	 attachment : {
	 raw stuff
	 …
	 }
	 },
	 …
	 ]
	
	 */
    
    /* parse the lines and group into one single JSON record, convert header fields to JSON, include map of raw data, return as a write operation */
    @Override
    public DocumentWriteOperation convert(List<String> lines) {
        DocumentWriteOperation writeOperation;
        List<String[]> fieldsAsArrays = new ArrayList<String[]>();
        for (String line : lines) {
        	fieldsAsArrays.add(line.split(Pattern.quote(String.valueOf(DELIMITER))));
        }      
        int len=fieldsAsArrays.size();
        if (len==0)
        	len+=0;
        writeOperation = operationOnId(fieldsAsArrays);
        return writeOperation;
    }

    private DocumentWriteOperation operationOnId(List<String[]> subItems) {
        String id = subItems.get(0)[idColumnIndex];
        if (id != null && "".equals(id)) {
            id = "MISSING";
        }
        String uri = "/" + summaryInfo.getEntityName() + "/" + id + "_" + UUID.randomUUID().toString() + ".json";

        String doc = serializeItem(id, subItems); // group the items into one overall JSON record
        if (doc == null) {
            return null;
        }

        StringHandle handle = new StringHandle(doc);
        handle.setFormat(Format.JSON);
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.withCollections(summaryInfo.getCollections().toArray(new String[0]));

        return new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, metadata, handle);
    }


	public String serializeItem(String id, List<String []> item) {
	        try {
	            SimpleDateFormat dateFormatParser = new SimpleDateFormat("MM/dd/yyyy");
	            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	
	            Map<String, Object> envelope = new HashMap<String, Object>();
	            if (item == null) {
	                return null;
	            }
	            int size = item.size();
	            JSONArray newInstanceArray = new JSONArray();
	
	            // loop through instances and create a new sparse instance for each
	            for (int i = 0; i < size; i++) {
	
	                Map<String, Object> rawInstance = arrayToMap(item.get(i));
	                if (rawInstance == null) {
	                    break;
	                }
	                String seqnum = (String) rawInstance.get("SEQNUM");
	
	                JSONObject entityObject = new JSONObject();
	                entityObject.put("SEQNUM", seqnum);
	
	                JSONObject instanceObject = new JSONObject();
	
	                for (String key : dateColumns) {
	                    //reformat date
	                	String dateText="non-initialized";
	                    try
	                    {
	                        dateText = (String) rawInstance.get(key);
	//                            4/11/2017
	
	                        Date date = dateFormatParser.parse(dateText);
	
	                        instanceObject.put(key+"_CONVERTED",dateFormatter.format(date));
	                    } catch (Exception e) {
	                        logger.warn("Error formatting date: '" +dateText + "' " + e.toString());
	                    }
	                }
	
	                entityObject.put("instance", instanceObject);
	                entityObject.put("attachment", rawInstance);
	                newInstanceArray.put(entityObject);
	            }
	
	            envelope.put("id", id);
	            envelope.put("headers", summaryRecord);
	
	            envelope.put(summaryInfo.getEntityName(), newInstanceArray);
	
	            String result = new JSONObject().put("envelope", envelope).toString();
	            envelope.clear();
	            return result;
	        } catch (JSONException e) {
	            throw new RuntimeException("Could not serialize as JSON ", e);
	        }
	}
	
	/* convert the array of strings to a map by using headerArray as the key names. map will be more similar to a JSONobject */
    protected Map<String,Object> arrayToMap(String[] strings) {
        HashMap<String,Object> map = new HashMap<>();
        for (int i=0; i<strings.length; i++) {
            map.put(headerArray[i], strings[i]);
        }
        return map;

    }


}