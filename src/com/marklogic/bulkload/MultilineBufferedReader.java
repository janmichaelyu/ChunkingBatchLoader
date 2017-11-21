package com.marklogic.bulkload;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Damon and jmichael on 11/14/17.
 */
public class MultilineBufferedReader {

    private static final int MAX_ID_LEN = 256;

    private final char DELIMITER;
    private final boolean hasHeader;
    final Logger logger = new Logger(); // TODO use standard pkg
    private String headerString = "HEADER NOT INITIALIZED";  // TODO not used
    private final int COLUMN_INDEX;
    private BufferedReader csvFile;
    int lineNo =0;
    
    private Path csvPath;
    
    public MultilineBufferedReader(Path csvPath, char delimiter, int columnIndex) {
    	this(csvPath, delimiter, columnIndex, true);
    }
    
    public MultilineBufferedReader(Path csvPath, char delimiter, int columnIndex, boolean hasHeader) {
        this.DELIMITER = delimiter;
        this.COLUMN_INDEX = columnIndex;
        this.hasHeader=hasHeader;
        csvFile = null;
        try {
        	this.csvPath = csvPath; // just so we can re-open csv in a loop for testing
            csvFile = new BufferedReader(new FileReader(csvPath.toFile()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (hasHeader) {
	        try {
	            this.headerString = csvFile.readLine();
	        } catch (IOException e) {
	        	logger.info("Error! failed to read header line: " + e.getMessage());
	            e.printStackTrace();
	        }
        }
    }

    public void readLinesToQueue(ReaderWriterQueue<List<String>> linesQueue)  {
    		// csvFile is already open, and header has been read off already
    		readLinesToQueueONCE(linesQueue);
    }
    
    public void readLinesToQueueONCE(ReaderWriterQueue<List<String>> linesQueue)  {
        logger.info("Reading");

        List<String> linesChunk = new ArrayList<String>(32);
        
        // look for the primary key
    	String previousId = "NO ID YET - at start of process";
    	String line = "NO LINE YET - at start of process";
    	boolean firstLine = true;
    	lineNo = 0;
    	
        while (true) {
        	// slow down for testing faster consume:     try{Thread.sleep(5);} catch (InterruptedException ex) {}
        	lineNo++;
        	if (lineNo % 100 == 0)
        		logger.info("Reader processing line: "+lineNo);
        	
        	try {
        		line = csvFile.readLine();
        		if (line == null) {
        			// end the last  chunk and push it to the write queue
        			if (linesChunk.size()>0) {
        				linesQueue.put(linesChunk);
        			}
        			break;
        		}
        		
        		String id = getId(line);
        		if ("".equals(id)) 
        			id = "MISSING_ID";
        		
                if ((id != null && id.equals(previousId)) || firstLine) { // same id, or very first ID in the file, then add
                	linesChunk.add(line); // accumulate in this chunk of lines, grouped by ID
                }
                else {
                	// done with this chunk of IDs. push the chunk out, and start the next chunk in a new List
                	linesQueue.put(linesChunk);
                	linesChunk = new ArrayList<String>(32);
                	linesChunk.add(line); // set as first item in the NEXT bunch of lines
                }
                
                firstLine = false;
                previousId = id;
        	} catch (IOException ex) {
        		logger.info("Error on line + '"+line+"' -- " + ex.getMessage());  // TODO log error stack
        	}
        }
        
        logger.info("READER DONE!");
    }

    private String getId(String line){
        int len = line.length();
        char c;
        int idx=0;

        for (int i=0;i<len;i++) {
            c = line.charAt(i);
            if (c == DELIMITER) idx++;
            if (idx == COLUMN_INDEX)
                return getStringToDelim(line, i);
        }

        return null;
    }

    private String getStringToDelim(String line, int i) {
        StringBuilder buf = new StringBuilder(MAX_ID_LEN);
        char c = 0;
        for (int j = i+1; c != DELIMITER && j < line.length(); j++) {
            c=line.charAt(j);
            if (c!=DELIMITER) {
                buf.append(c);
            } else {
                break;
            }
        }

        if (logger.isLoggable()) // Level.FINE))
        	logger.finer("Getting ID from line '"+line+"' = '"+buf.toString()+"'");

        return buf.toString();
    }

}