package com.marklogic.bulkload;

public class Logger {

	public void info(String msg) {
		//System.out.println(msg);
	}	
	
	public void debug(String msg) {
		//System.out.println(msg);
	}
	
	public void finer(String msg) {
		//System.out.println(msg);
	}	
	
	public void warn(String msg) {
		System.out.println(msg);
	}
	
	public boolean isLoggable() {
		return false;
	}
}
