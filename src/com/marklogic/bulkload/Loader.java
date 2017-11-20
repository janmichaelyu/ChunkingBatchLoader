package com.marklogic.bulkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.marklogic.bulkload.convert.DocumentFormatConverter;
import com.marklogic.bulkload.convert.FormatConverter;
import com.marklogic.bulkload.convert.SummaryInfo;
import com.marklogic.bulkload.convert.UnparsedLinesWriteConverter;
import com.marklogic.bulkload.read.MultilineBufferedReader;
import com.marklogic.bulkload.write.LineQueueConsumer;
import com.marklogic.bulkload.write.XccAllAsJSONToMLSvcChunkWriter;
import com.marklogic.bulkload.write.XccChunkWriter;
import com.marklogic.bulkload.write.XccRawLinesInJSONStructureChunkWriter;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;

public class Loader<T> {

	static final Logger logger = new Logger(Loader.class);

	private static final int QUEUE_SIZE = 64; // the queue to hold read lines. max no before blocking
	
	static  String pathString = "/temp/inpatientLarger.txt"; // TODO hard coded !!
	final Path path; // TODO initialize with the string from spring or similar
	final char delimiter = '\t'; // TODO hard coded !!
	final int idColumnIndex = 35; // TODO hard coded !!!
	final int batchSize = 10; // TODO hard coded !!
	String[] headers;
	private String entityName = "drug"; // TODO hard coded !!
	
	String host = "localhost"; // TODO hard coded
	int port = 8000; // TODO hard coded
	String user = "admin"; // TODO hard coded
	String pass = "admin"; // TODO hard coded
	
	List<LineQueueConsumer<T>> writers = new ArrayList<LineQueueConsumer<T>>();
	List<Thread> writerThreads = new ArrayList<Thread>();
	
	// helper classes
	FormatConverter<T> converter; // turns strigns to json
	List<ContentSource> contentSources; // xcc class for connections
	
	final ReaderWriterQueue<List<String>> linesQueue;

	protected String[] collections = new String[] {"Inpatient", "TESTING"};

	protected Set<String> columnsOfInterest;
	
	private enum ConfigType {RawLinesToDSH, JSONPerItem, JSONForAllItemsInChunk};
	
	private static final ConfigType CONFIG_TYPE = ConfigType.RawLinesToDSH;  // DRIVES THE OVERALL CONFIG

	private XccChunkWriter<T> chunkWriter;
	
	public static final boolean TEST_MODE = true;
	
	public static void main(String[] args) {
        
		if (args.length > 0) {
			pathString = args[0];
		}
		
		int threadCount = 16; // hardcoded !!
		if (args.length > 1) {
			String threadStr = args[0];
			threadCount = Integer.parseInt(threadStr);
		}		

		// set up a single-threaded loader. It is critical that this single-threaded part run as fast as possible.
		// the loader reads lines and queues them on this queue. the queue is the communications mechanism to the writers.
	    Path path = Paths.get(pathString);
		Loader loader = new Loader(path);
		loader.init(); // set the header metadata, etc.

	    // start timing as we kick off the threads
		long start = System.currentTimeMillis();
		
	    // --- WRITERS ----  now set up threads for the writers
	    // these are "consumers" of the lines queue in the producer/consumer pattern. The loader is the producer.
		List<Thread> writerThreads =null;
	    for (int i=0;i<threadCount;i++) {
	    	writerThreads = loader.startWriterThread(loader.linesQueue, i);
	    }
	    
	    // ---- READER ----- now start the read process
	    Thread lt = loader.startLoadThread();
	    
	    
	    // -- CLEANUP --
	    logger.info("Loader and all writers started. Waiting for completion...");
		  
	    waitForLoaderThreadComplete(lt);
   
	    loader.waitForAllWritersComplete(loader, writerThreads);    
	    
	    long stop = System.currentTimeMillis();
	    System.out.println("DONE! active time = "+(stop-start)+" ms");

	    logger.info("Sanity check: all lines written="+LineQueueConsumer.allLinesWritten.size());
	    logger.info("Sanity check: all gets="+loader.linesQueue.totGet);
	    logger.info("Sanity check: all PUTs="+loader.linesQueue.totPut);
	    
	}

	private void waitForAllWritersComplete(Loader loader, List<Thread> writerThreads) {
		for (;;) {
	    	loader.linesQueue.alowAnotherGet(); // for each thread, let it wake up and do another check to see the queue is marked for completion
	    	// TESTING DELAY - see how it runs for a slow reader     try {Thread.sleep(5);} catch (InterruptedException ex) {}
	    	boolean allDone = true;
	    	for(int i=0; i< writers.size();i++) {
	    		LineQueueConsumer<T> writer = writers.get(i);
	    		if (writer != null && !writer.isDone()) { allDone=false; }
	    	}
	    	if (allDone) break;
	    }
	    
	    for (Thread t : writerThreads) {
	    	waitForLoaderThreadComplete(t);
	    }
	}

	private static void waitForLoaderThreadComplete(Thread lt) {
		try {lt.join();} catch (InterruptedException ex) {} // wait for the loader to finish
	}

	public Loader (Path path) {
		this.path=path;	
		this.linesQueue = new ReaderWriterQueue<List<String>>(QUEUE_SIZE);
	}
	
	private void init() {

		String headerLine=readHeaderLine(path);
	    
	    columnsOfInterest = buildColumnsOfInterest();
	    
	    // Summary info is basically the envelope header
	    SummaryInfo summary = new SummaryInfo(
	    		entityName,
	    		Arrays.asList(collections),
	    		"TODOSourceDescInLoader", // TODO find this
	    		pathString,
	    		pathString,
	    		Long.toString(new File(pathString).getTotalSpace()),
	    		headerLine,
	    		"TODOIMportID" // TODO find this
	    		);
	    
	    contentSources = getContentSources();

	    // Abstract Factory Pattern - build all the related subclasses that work together (e.g. writer and converter must match)

	    // Converter: builds the envelope with headers, and adds collections, URI etc.
	    if (CONFIG_TYPE == ConfigType.RawLinesToDSH ) {
	    	converter  = (FormatConverter<T>) new UnparsedLinesWriteConverter(headers, idColumnIndex, columnsOfInterest, summary); 
	    	chunkWriter = (XccChunkWriter<T>) new XccRawLinesInJSONStructureChunkWriter(contentSources);
	    }
	    else if (CONFIG_TYPE == ConfigType.JSONPerItem ) {
	    	converter  = (FormatConverter<T>) new DocumentFormatConverter(headers, idColumnIndex, columnsOfInterest, summary);
	    	chunkWriter = null; // copy from other branch?  per-item process where we use xcc.insertContent(content[]) is lost
	    	throw new UnsupportedOperationException("Cannot do JSONPerItem right now");
	    }
	    else if (CONFIG_TYPE == ConfigType.JSONForAllItemsInChunk ) {
	    	converter  = (FormatConverter<T>) new DocumentFormatConverter(headers, idColumnIndex, columnsOfInterest, summary);
	    	chunkWriter = (XccChunkWriter<T>) new XccAllAsJSONToMLSvcChunkWriter(contentSources);
	    }
	    else
	    	throw new RuntimeException("Unknown configuration type: "+CONFIG_TYPE);
	    		
	}
	
	// TODO hardcoded at three sources !!
	private List<ContentSource> getContentSources() {
		ContentSource cs1 = ContentSourceFactory. newContentSource(host, port, user, pass);
		ContentSource cs2 = ContentSourceFactory. newContentSource(host, port, user, pass);
		ContentSource cs3 = ContentSourceFactory. newContentSource(host, port, user, pass);
		List<ContentSource> sources = new ArrayList<ContentSource>();
		sources.add(cs1);
		sources.add(cs2);
		sources.add(cs3);
		return sources;
	}

	private static Set<String> buildColumnsOfInterest() {
		Set<String> cols = new HashSet<>();
		cols.add("ADMDATE");
		cols.add("DISDATE");
		// TODO hardcoded!! get list from properties. Or... can we scan all headers and assume *DATE is a date?
		return cols;
	}
	
	private String readHeaderLine(Path path) {
		String headerLine = null;
		try {
          BufferedReader fileReader = new BufferedReader(new FileReader(path.toFile()));
          headerLine = fileReader.readLine();
          headers = headerLine.split(Pattern.quote(String.valueOf(delimiter)));
	    } catch (IOException ex) {
	    	logger.warn("Error getting headers" + ex);
	    	throw new RuntimeException("Could not open file and read headers");
	    }
		return headerLine;
	}

	public Thread startLoadThread() {
		Thread t;
	    Runnable r = new Runnable() {
			@Override
			public void run() {
				MultilineBufferedReader reader = new MultilineBufferedReader(path, delimiter, idColumnIndex);
				reader.readLnesToQueue(linesQueue);
				logger.info("Loader Done!! Thread shoudl now terminate. gets="+linesQueue.totGet+ " puts="+linesQueue.totPut);
				linesQueue.setLoadingOver(); // signals readers that they should empty the queue and complete
			}

		};
		t= new Thread(r, "LoaderThread-singleton");
		t.start();
		return t;
		
	}	
	
	// kick off the writers, which will consume from the shared queue and write to ML
	public List<Thread> startWriterThread(
					ReaderWriterQueue<List<String>> linesQueue, int i) {
	    Runnable r = new Runnable() {

			@Override
			public void run() {
				LineQueueConsumer writer = new LineQueueConsumer(linesQueue, batchSize, converter, chunkWriter, contentSources); // TODO to support alternate write strategies, make an interface LineWriter
				writers.add(writer); // So we can stop them later
				writer.writeTillDone();
			}
		};
		Thread t = new Thread(r, "WriterThread-"+i);
		writerThreads.add(t);
		t.start();
		return writerThreads;
	}
		
}
