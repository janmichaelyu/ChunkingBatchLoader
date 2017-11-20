package com.marklogic.bulkload.write;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.marklogic.bulkload.Logger;
import com.marklogic.bulkload.ReaderWriterQueue;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.xcc.ContentSource;

import convert.DocumentFormatConverter;
import convert.FormatConverter;
import convert.UnparsedLinesWriteConverter;

/**
 * Consumes data from a shared queue and writes it. 
 * @author dfeldman
 *
 */

public class LineQueueConsumer<T> {

	public static List<String> allLinesWritten = Collections.synchronizedList(new ArrayList());  // for debugging see if all the lines made it
	boolean done; // signal the  main thread that we can exit
	
	private ReaderWriterQueue<List<String>> lineQueue;
	int n = -1;
	int count = 0;
	int batchSize;
	FormatConverter<T> converter;
	
	Logger logger = new Logger(LineQueueConsumer.class); // TODO revisit logging
	private XccChunkWriter<T> xccWriter;

	
	public LineQueueConsumer(
				ReaderWriterQueue<List<String>> lineArrayQueue, int batchSize, 
				FormatConverter<T> converter, XccChunkWriter<T> chunkWriter, List<ContentSource> contentSources) {
		this.lineQueue = lineArrayQueue; 
		this.batchSize = batchSize;
		this.converter = converter;
		
		if (converter==null) throw new RuntimeException("No conveter."); // fail early on main thread

		// TODO !! find the best delegate:
		// DELEGATE - change this to change write strategy
		xccWriter = chunkWriter;
	}
	
	public void writeTillDone() {
		int sentCount =0;
		List<T> batch = new ArrayList<T>(128); // represents a batch of records. Each record will have all lines for one ID
		for(;;) {
			List<String> linesForOneDoc = lineQueue.get();
			count++;
			if (count % 100 == 0)
				logger.info("  - Writer "+n+" consumed "+count+" chunks. About to write last one.");
			
			boolean queueEmpty = linesForOneDoc==null? true : false;
			if (queueEmpty) {  // empty means we are done. If the reader is just behind the queue will block the get() call
				logger.info("Writer "+n+" Stopping after consuming "+count+ " lines.  *** about to write last batch size="+batch.size());
				xccWriter.write(batch);
				done=true;
				return;
			}			
			
			T writeOp = converter.convert(linesForOneDoc);
			batch.add(writeOp);
			
			if (batch.size() >= this.batchSize) {
				sentCount+=batch.size();
				if (logger.isLoggable())
					logger.debug("Batch ready for writing. size="+batch.size() + "t="+Thread.currentThread().getName() + " tot to write="+sentCount);
				// TODO write it!
				xccWriter.write(batch);
				batch.clear();
			}
			
			// DEBUG ONLY 
			// TODO: remove for faster performance
			allLinesWritten.addAll(linesForOneDoc);

		}
	}

	public boolean isDone() {
		return done;
	}
}
