package com.marklogic.bulkload.convert;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.marklogic.bulkload.Logger;

public class SummaryInfo {


	public SummaryInfo(String entityName, List<String> collections, String sourceDescription, String sourcePath, String fullHadoopPath,
			String fileSize, String tableHeader, String importId) {
		this.entityName = entityName;
		this.collections = collections;
		this.sourceDescription = sourceDescription;
		this.sourcePath = sourcePath;
		this.fullHadoopPath = fullHadoopPath;
		this.fileSize = fileSize;
		this.tableHeader = tableHeader;
		this.importId = importId;
	}

	private List<String> collections;
	private String entityName;
	private String sourceDescription;
	private String sourcePath;
	private String fullHadoopPath;
	private String fileSize;
	private String tableHeader;
	private String importId;
	private String ingestionStartTime = getISO8601StringForDate(new Date());

	private Logger logger = new Logger(SummaryInfo.class);
	


	public SummaryInfo(
				List<String> collections, String entityName, String sourceDescription, 
				String sourcePath, String fullHadoopPath, String fileSize, 
				String tableHeader, String importId) {
		super();
		this.collections = collections;
		this.entityName = entityName;
		this.sourceDescription = sourceDescription;
		this.sourcePath = sourcePath;
		this.fullHadoopPath = fullHadoopPath;
		this.fileSize = fileSize;
		this.tableHeader = tableHeader;
		this.importId = importId;
	}


	public JSONObject toJson() {
		String importID = UUID.randomUUID().toString();
		try {
			JSONObject summaryRecord = new JSONObject() {
				@SuppressWarnings("unused")
				private static final long serialVersionUID = 6202992963946072637L;
				{
					put("SourceName", entityName);
					put("SourceDescription", sourceDescription);
					put("SourcePath", sourcePath);
					put("FullHadoopPath", fullHadoopPath);
					put("FileSize", fileSize);
					put("TableHeader", tableHeader);
					put("IngestionStartDateTime", ingestionStartTime);
					put("ImportID", importID);
				}
			};
			return summaryRecord;
		} catch (JSONException e) {
			logger.warn("Error creating JSON header info: "+ e);
			return null;
		}
	}
	

	public String getEntityName() {
		return entityName;
	}
	
	public List<String> getCollections() {
		return collections;
	}

	public String getSourceDescription() {
		return sourceDescription;
	}

	public String getSourcePath() {
		return sourcePath;
	}

	public String getFullHadoopPath() {
		return fullHadoopPath;
	}

	public String getFileSize() {
		return fileSize;
	}

	public String getTableHeader() {
		return tableHeader;
	}

	public String getImportId() {
		return importId;
	}

	public String getIngestionStartTime() {
		return ingestionStartTime;
	}
	
	private static String getISO8601StringForDate(Date date) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormat.format(date);
	}

}
