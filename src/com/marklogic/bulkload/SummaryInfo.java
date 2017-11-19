package com.marklogic.bulkload;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class SummaryInfo {


	public SummaryInfo(String entityName, String sourceDescription, String sourcePath, String fullHadoopPath,
			String fileSize, String tableHeader, String importId) {
		this.entityName = entityName;
		this.sourceDescription = sourceDescription;
		this.sourcePath = sourcePath;
		this.fullHadoopPath = fullHadoopPath;
		this.fileSize = fileSize;
		this.tableHeader = tableHeader;
		this.importId = importId;
	}

	private String entityName;
	private String sourceDescription;
	private String sourcePath;
	private String fullHadoopPath;
	private String fileSize;
	private String tableHeader;
	private String importId;
	private String ingestionStartTime = getISO8601StringForDate(new Date());

	private Logger logger = new Logger();
	
	public SummaryInfo() {
		// from props:  description, inputFile
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
