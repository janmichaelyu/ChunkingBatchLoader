package com.marklogic.bulkload;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
 
public class ReaderWriterQueue<T> {

	private Semaphore put = new Semaphore(1);
	private Semaphore get = new Semaphore(1);
	
	 int totPut = 0;
	 int totGet = 0;
	
	private static final Logger logger = new Logger();
	
	private short maxSize;
	private boolean loadingOver=false; // set to true when there is no more data expected
	
	Queue<T> queue = new ConcurrentLinkedDeque<T>();  // TODO use an Array of type <T> for speed. because the size is limited, we can do a ring buffer.
	short size = 0;
	
	public ReaderWriterQueue(int size) {
		maxSize = (short)size;
	}
	
	public synchronized boolean isLoadingOver() {
		return loadingOver;
	}
	public synchronized void setLoadingOver() {
		logger.info(" (((()))  setLoggingOver called. totPut="+totPut);
		loadingOver = true;
	}
	
	
	/**
	 * Get an item. If the queue is empty and no more data is loading, return null
	 * @return item or null
	 */
	public T get() { // TODO when it's an array, optimized get(N)
		T item = null;
		for(;;) {
			synchronized (this) {
				item = queue.poll();
			}
			if (item==null && !isLoadingOver()) {
				get.acquireUninterruptibly(); // buffer empty. wait till someone wakes this thread up with a put, len loop and try again
			} else if (item==null && isLoadingOver()){
				return null; // if null => signal that we are REALLY done. Loading is over, and all items consumed 
			} else if (item!=null) {
				synchronized(this) {
					size--;
					totGet++;
					put.release(); // returning a real item, so must have consumed one. wake up anyone waiting because buffer is full
					return item;				
				}
			}
		}
		// throw new RuntimeException("Unreachable.");
	}
	
	public void put(T item) {
		if (size>maxSize) 
			put.acquireUninterruptibly(); // buffer full. wait.
		synchronized (this) {
			queue.add(item);
			totPut++;
			size++;
			get.release();  // if consumers (xccwriters) are blocked getting data to write, release them
		}
	}	
	

	/** if this is blocked at the end of the process, the caller may explicitly tell it to read a littel more to see if the process is done */
	public void alowAnotherGet() {
		get.release();
	}
}
