package com.marklogic.bulkload;


import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.DocumentFormat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * XCC implementation to write a chunk of docs in one call. Most important thing here is we depend on an instance of
 * DocumentWriteOperationAdapter to adapt  a DocumentWriteOperation instance into a Content instance.
 * 
 * TODO this may be better if it groups all JSON docs into a master doc, sends to an .xqy endpoint that invokes a DHF input flow on each sub-doc
 */
public abstract class XccChunkWriter {

	private ContentSourceIterator csIterator;
	private int contentSourceIndex = 0;
	static int count = 0;
	int myCount=0;
	
	Logger logger = new Logger(); // TODO use better logging
	
	public XccChunkWriter(List<ContentSource> contentSources) {
		this.csIterator = new ContentSourceIterator(contentSources.toArray(new ContentSource[0]));
	}

	public void write(List<DocumentWriteOperation> items) {
		myCount+=items.size();
		logger.info("  about to write. will have seen "+myCount+" items. t="+Thread.currentThread().getName());
		
		
		ContentSource contentSource = csIterator.next();
		
		// convert from DMSDK-style Java API objects to XCC objects 
		int len = items.size();
		Content[] contents = new Content[len];
		
		doWrite(items, contentSource);
		
		synchronized(XccChunkWriter.class) {
			count += items.size(); 
			logger.info("total processed by xccbatchwriter ="+count+" xcclinewrit.alllines="+XCCLineWriter.allLinesWritten.size());
		}
	
	}

	protected abstract void doWrite(List<DocumentWriteOperation> items, ContentSource contentSource);
	
	protected ContentCreateOptions adaptMetadata(DocumentMetadataWriteHandle handle) {
		ContentCreateOptions options = new ContentCreateOptions();
		if (handle instanceof DocumentMetadataHandle) {
			DocumentMetadataHandle metadata = (DocumentMetadataHandle) handle;
			options.setQuality(metadata.getQuality());
			options.setCollections(metadata.getCollections().toArray(new String[]{}));
			adaptPermissions(options, metadata);
			adaptFormat(options, metadata);
		} else {
			logger.warn("Only supports DocumentMetadataHandle; unsupported metadata class: " + handle.getClass().getName());
		}
		return options;
	}
	

	protected void adaptFormat(ContentCreateOptions options, DocumentMetadataHandle metadata) {
		Format format = metadata.getFormat();
		DocumentFormat xccFormat = null;
		if (format != null) {
			if (Format.BINARY.equals(format)) {
				xccFormat = DocumentFormat.BINARY;
			} else if (Format.JSON.equals(format)) {
				xccFormat = DocumentFormat.JSON;
			} else if (Format.TEXT.equals(format)) {
				xccFormat = DocumentFormat.TEXT;
			} else if (Format.XML.equals(format)) {
				xccFormat = DocumentFormat.XML;
			} else if (Format.UNKNOWN.equals(format)) {
				xccFormat = DocumentFormat.NONE;
			} else if (logger.isLoggable()) {
				logger.debug("Unsupported format, can't adapt to an XCC DocumentFormat; " + format.toString());
			}
		}
		if (xccFormat != null) {
			if (logger.isLoggable()) {
				logger.debug("Adapted REST format " + format + " to XCC format: " + xccFormat.toString());
			}
			options.setFormat(xccFormat);
		}
	}

	protected void adaptPermissions(ContentCreateOptions options, DocumentMetadataHandle metadata) {
		Set<ContentPermission> contentPermissions = new HashSet<>();
		DocumentMetadataHandle.DocumentPermissions permissions = metadata.getPermissions();
		for (String role : permissions.keySet()) {
			for (DocumentMetadataHandle.Capability capability : permissions.get(role)) {
				ContentCapability contentCapability;
				if (DocumentMetadataHandle.Capability.EXECUTE.equals(capability)) {
					contentCapability = ContentCapability.EXECUTE;
				} else if (DocumentMetadataHandle.Capability.INSERT.equals(capability)) {
					contentCapability = ContentCapability.INSERT;
				} else if (DocumentMetadataHandle.Capability.READ.equals(capability)) {
					contentCapability = ContentCapability.READ;
				} else if (DocumentMetadataHandle.Capability.UPDATE.equals(capability)) {
					contentCapability = ContentCapability.UPDATE;
				} else throw new IllegalArgumentException("Unrecognized permission capability: " + capability);
				contentPermissions.add(new ContentPermission(contentCapability, role));
			}
		}
		options.setPermissions(contentPermissions.toArray(new ContentPermission[]{}));
	}

	public static class ContentSourceIterator implements Iterator<ContentSource> {
		private ContentSource[] contentSources;
		short idx =0;
		public ContentSourceIterator(ContentSource[] css) {
			this.contentSources = css;
		}
		
		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public ContentSource next() {
			idx++;
			if (idx > contentSources.length - 1);
				idx = 0;
			return contentSources[idx];
		}
		
	}
}