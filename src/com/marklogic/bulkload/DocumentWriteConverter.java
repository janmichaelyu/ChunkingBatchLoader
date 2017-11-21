package com.marklogic.bulkload;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by jmichael on 11/9/17.
 */
public class DocumentWriteConverter {

    private static final char DELIMITER = '\t';   // TODO hardcoded !!
	private Logger logger = new Logger(); // Logger.getLogger(this.getClass().getName());
    private String[] headerArray;
    private final int idColumnIndex;
    private String entityName;
    private String[] collections;
    private Set<String> columnsOfInterest;
    private JSONObject summaryRecord;
    private ArrayList<String> dateColumns;

    public DocumentWriteConverter(String[] headerArray,
                                  int idColumnIndex,
                                  String entityName,
                                  String[] collections,
                                  Set<String> columnsOfInterest,
                                  SummaryInfo summaryInfo) {
        this.headerArray = headerArray;
        this.idColumnIndex = idColumnIndex;
        this.entityName = entityName;
        this.collections = collections;
        this.columnsOfInterest = columnsOfInterest;
        this.summaryRecord = summaryInfo.toJson();

        dateColumns = new ArrayList();
        for (String column : columnsOfInterest){
            if (column.endsWith("DATE")) {
                dateColumns.add(column);
            }
        }
    }

    /* parse the lines and group into one single JSON record, convert to JSON, return as a  write operation */
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
        logger.info("id: "+id);
        String uri = "/" + entityName + "/" + id + "_" + UUID.randomUUID().toString() + ".json";

        String doc = serializeItem(id, subItems); // group the items into one overall JSON record
        if (doc == null) {
            return null;
        }

        StringHandle handle = new StringHandle(doc);
        handle.setFormat(Format.JSON);
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.withCollections(collections);

        return new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, metadata, handle);
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

            envelope.put(entityName, newInstanceArray);

            String result = new JSONObject().put("envelope", envelope).toString();
            envelope.clear();
            return result;
        } catch (JSONException e) {
            return null;
        }
    }

    private Map<String,Object> arrayToMap(String[] strings) {
        HashMap<String,Object> map = new HashMap<>();

        for (int i=0; i<strings.length; i++) {
            map.put(headerArray[i], strings[i]);
        }

        return map;

    }


}