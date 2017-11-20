package com.marklogic.bulkload.write;

public class Item<T> {

	private T content;
	
	public Item(T content) {
		this.content=content;
	}
	
	public T getContent() {
		return content;
	}
}
