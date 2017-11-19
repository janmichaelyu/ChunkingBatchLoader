package com.marklogic.bulkload;

import java.util.List;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;

/** 
 * Writes a chunk of documents by creating a single XCC contentInsert call with all of them.
 * 
 * @author Damon
 *
 */
public class XccMulitItemInsertChunkWriter extends XccChunkWriter {

	public XccMulitItemInsertChunkWriter(List<ContentSource> contentSources) {
		super(contentSources);
	}

	protected void doWrite(List<DocumentWriteOperation> items, ContentSource contentSource) {
		int len = items.size();
		Content[] contents = new Content[len];
		for (int i=0; i<len; i++) {
			DocumentWriteOperation op = items.get(i);
			AbstractWriteHandle h = op.getContent();
			ContentCreateOptions options = adaptMetadata(op.getMetadata());
			Content c = ContentFactory.newContent(op.getUri(), ((StringHandle) h).get(), options);
            c.getCreateOptions().setFormatJson();

			contents[i] = c;
		}
		
		// Write the data!
		try {
			contentSource.newSession().insertContent(contents);
		} catch (Exception ex) {
			throw new RuntimeException("Unable to insert content set ", ex);
		}
	}

}
