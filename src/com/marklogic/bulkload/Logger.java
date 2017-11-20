package com.marklogic.bulkload;

// TODO: delegate to a real logging package, and set up logging config properly
public class Logger {

	public Logger(Class clazz) {
		// ignore for now, bu use class name to configure a real logger
	}
	
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
