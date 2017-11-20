package com.marklogic.bulkload.write;

import java.util.List;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;

/** 
 * Writes a chunk of documents by creating one large JSON structure holding all docs (with URIs and metadata) and sending 
 * them to an .xqy endpoint with URI "/batchHubInput.xqy" which inserts each one separately.
 * 
 * @author Damon
 *
 */

public class XccAllAsJSONToMLSvcChunkWriter extends XccChunkWriter<DocumentWriteOperation> {

	private static final String BATCH_HUB_INPUT_XQY = "/batchHubInput.xqy";

	public XccAllAsJSONToMLSvcChunkWriter(List<ContentSource> contentSources) {
		super(contentSources);
	}

	@Override
	protected void doWrite(List<DocumentWriteOperation> items, ContentSource contentSource) {
		int len = items.size();
		if (len==0) {
				logger.debug("Empty chunk passed to XccMultiItemWriter.doWrite. skipping.");
				return;
		}
		
		//TODO use JSON library, not string buffer
		// serialize all the content with URIs to one big JSON string
		StringBuffer sb = new StringBuffer(1024);
		sb.append("{\"metadata\": ").append("{},");  // TODO !!! put the permissions and collections in here
		sb.append(" \"data\": ");  // TODO !!! put the permissions and collections in here
		sb.append("["); // start a JSON list
		for (int i=0; i<len; i++) {
			DocumentWriteOperation op = (DocumentWriteOperation) items.get(i);
			sb.append("{\"URI\": ").append('"').append(op.getUri()).append('"').append(',') ;  // {"URI": "the-actual-uri",
			AbstractWriteHandle h = op.getContent();
			String jsonString = ((StringHandle) h).get();
			sb.append("\"item\": ").append(jsonString).append('}').append(',') ;  // "item": "{json:..}"},
		}
		sb.setLength(sb.length()-1); // discard the final comma
		sb.append("]}"); // close the array, close the overall object
		String all = sb.toString();
		
		logger.info("all  "+all);
		
		// TODO
		// TODO !!!!!  encode the metadata and collections into the request, parse in MarkLogic and set on insert!!
		// TODO
		// Note we use DMSDK data structueres to maintain compatibility with DMSDK, though we currently optimize with xcc
		DocumentWriteOperation first = (DocumentWriteOperation) items.get(0);
		DocumentMetadataWriteHandle md = first.getMetadata();
		ContentCreateOptions options = adaptMetadata(first.getMetadata());
		// TODO!!
				
		// Write the data!
		try {
			Session sess = contentSource.newSession();
			Request r = sess.newModuleInvoke(BATCH_HUB_INPUT_XQY); 
			r.setNewStringVariable("allItems", all);
			sess.submitRequest(r);
		} catch (Exception ex) {
			throw new RuntimeException("Unable to insert content set ", ex);
		}
	}

}
