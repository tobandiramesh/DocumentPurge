package com.datum.purge.tool.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.datum.purge.tool.DocumentPurgeTool;
import com.filenet.api.collection.ContentElementList;
import com.filenet.api.collection.DocumentSet;
import com.filenet.api.constants.AutoClassify;
import com.filenet.api.constants.AutoUniqueName;
import com.filenet.api.constants.CheckinType;
import com.filenet.api.constants.DefineSecurityParentage;
import com.filenet.api.constants.PropertyNames;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.ReferentialContainmentRelationship;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;

public class DocumentPurgeExecutor extends Thread {

	public String CLASS_NAME = "DocumentPurgeExecutor";
	public static int ROW_COUNT = 0;
	public static StringBuilder REPORT_DATA = new StringBuilder();
	SimpleDateFormat DATE_PATTERN = new SimpleDateFormat("MM/dd/yyyy");
	public String REPORT_NAME = "DeletionReport_" + new Date().getTime() + ".csv";
	private ReentrantLock reentrantLock = new ReentrantLock();

	@Override
	public void run() {

		StringBuilder searchSQLQuery = null;

		String methodName = "run";
		String csvDataLine = "";
		String splitDelimeter = ",";
		String[] fieldOperators = DocumentPurgeConfigLoader.ce_field_operators.split(splitDelimeter);
		String[] searchFields = DocumentPurgeConfigLoader.ce_SearchFields.split(splitDelimeter);
		String[] reportHeaders = DocumentPurgeConfigLoader.report_headers.split(splitDelimeter);

		try {
			ObjectStore objectStore = new DocumentPurgeUtil().fetchObjectStore();
			while ((csvDataLine = DocumentPurgeTool.CSV_DATA_READER.readLine()) != null) {

				String[] csvDataValues = csvDataLine.split(splitDelimeter);

				searchSQLQuery = new StringBuilder();
				searchSQLQuery.append("Select * from [");
				StringBuilder rowStatus = new StringBuilder();

				for (int csvDataCount = 0; csvDataCount < csvDataValues.length; csvDataCount++) {

					if (csvDataCount > 0) {

						String ceSearchFieldDataType = searchFields[csvDataCount - 1];

						if (ceSearchFieldDataType.contains("|")
								&& "date".equalsIgnoreCase(ceSearchFieldDataType.split("\\|")[1])) {
							searchSQLQuery.append(ceSearchFieldDataType.split("\\|")[0] + " "
									+ fieldOperators[csvDataCount - 1] + " ");
							searchSQLQuery.append(getConvertedDate(csvDataValues[csvDataCount])).append("");

						} else if (ceSearchFieldDataType.contains("|")
								&& "number".equalsIgnoreCase(ceSearchFieldDataType.split("\\|")[1])) {
							searchSQLQuery.append(ceSearchFieldDataType.split("\\|")[0] + " "
									+ fieldOperators[csvDataCount - 1] + " ");
							searchSQLQuery.append(csvDataValues[csvDataCount]).append("");

						} else {
							searchSQLQuery.append(ceSearchFieldDataType.split("\\|")[0] + " "
									+ fieldOperators[csvDataCount - 1] + " '");
							searchSQLQuery.append(csvDataValues[csvDataCount]).append("'");
						}

						rowStatus.append(csvDataValues[csvDataCount]);
						rowStatus.append(",");

						if (csvDataCount != csvDataValues.length - 1)
							searchSQLQuery.append(" AND ");

					} else {
						searchSQLQuery.append(csvDataValues[csvDataCount]);
						searchSQLQuery.append("] where ");

						rowStatus.append(csvDataValues[csvDataCount]);
						rowStatus.append(",");
					}
				}

				int iCSVDataValues = csvDataValues.length - 1;
				if (iCSVDataValues < searchFields.length) {
					for (int i = 0; i < searchFields.length - iCSVDataValues; i++) {
						rowStatus.append(",");
					}
				}

				DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
						"Executing Search Query : [" + searchSQLQuery.toString() + "]");
				SearchSQL searchSQL = new SearchSQL(searchSQLQuery.toString());
				SearchScope search = new SearchScope(objectStore);

				DocumentSet documentSet = (DocumentSet) search.fetchObjects(searchSQL, 100, null, true);

				if (!documentSet.isEmpty()) {
					Iterator<?> it = documentSet.iterator();

					int docCount = 0;

					while (it.hasNext()) {
						Document document = (Document) it.next();

						try {
							document.fetchProperties(new String[] { PropertyNames.ID });
							String name = document.get_Name();
							String id = document.get_Id().toString();

							document.delete();
							document.save(RefreshMode.REFRESH);
							docCount++;
							DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
									"\n Document Name: " + name + " Document Id: " + id + " has been deleted.");

						} catch (Exception exception) {

							DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
									exception.getMessage());
						}
					}

					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"\n Total {" + docCount + "} Document(s) have been deleted for " + csvDataLine);

					rowStatus.append(docCount);
					rowStatus.append(",");
					rowStatus.append(" document(s) deleted ");

				} else {
					rowStatus.append(0);
					rowStatus.append(",");
					rowStatus.append(" No documents found ");

					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"\n No documents have been found for " + csvDataLine);
				}
				ROW_COUNT++;

				rowStatus.append("\n");
				REPORT_DATA.append(rowStatus.toString());

				if (ROW_COUNT == DocumentPurgeTool.ROW_TOTAL_COUNT) {

					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"Report Data \n" + REPORT_DATA);
					FileWriter csvWriter = new FileWriter(REPORT_NAME);
					csvWriter.append("Document Class");
					csvWriter.append(",");

					for (int reportHeaderCount = 0; reportHeaderCount < reportHeaders.length; reportHeaderCount++) {
						csvWriter.append(reportHeaders[reportHeaderCount]);
						csvWriter.append(",");
					}

					csvWriter.append("No'of Docs");
					csvWriter.append(",");
					csvWriter.append("Status");
					csvWriter.append("\n");
					csvWriter.append(REPORT_DATA);
					csvWriter.flush();
					csvWriter.close();
					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"Report has been generated.");
					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"DocumentPurgeTool execution has been completed.");

					// Upload the generated report to FileNet
					if (reentrantLock.tryLock(0, TimeUnit.SECONDS)) {
						UploadReport(new File(REPORT_NAME), objectStore);
					}					
				}
			}
		} catch (Exception exception) {

			DocumentPurgeLogger.writeErrorLog(CLASS_NAME, methodName, "Exception Occured", exception);
			DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
					"\n Report data \n" + REPORT_DATA);

		}
	}

	@SuppressWarnings("unchecked")
	private void UploadReport(File file, ObjectStore objectStore) {

		Document doc = Factory.Document.createInstance(objectStore, DocumentPurgeConfigLoader.ce_upload_docClass);
		doc.getProperties().putValue("DocumentTitle", REPORT_NAME);

		// Set content
		ContentElementList contentList = Factory.ContentElement.createList();
		ContentTransfer ct = Factory.ContentTransfer.createInstance();
		InputStream inputStream = null;
		if (file.exists()) {
			try {
				inputStream = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				System.err.println(file.getAbsolutePath() + " does not exist.");
				e.printStackTrace();
			}
			ct.setCaptureSource(inputStream);
			ct.set_RetrievalName(REPORT_NAME);
			contentList.add(ct);
			doc.set_ContentElements(contentList);
			doc.set_MimeType("text/plain");
			doc.save(RefreshMode.NO_REFRESH);
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			doc.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
			doc.save(RefreshMode.NO_REFRESH);
			String[] props = { "ID", "ClassDescription" };
			doc.fetchProperties(props);
			System.out.println("Report uploaded: " + doc.get_Id());

			// File the document into folder
			com.filenet.api.core.Folder folder = Factory.Folder.getInstance(objectStore, "Folder",
					DocumentPurgeConfigLoader.ce_upload_folderPath);
			ReferentialContainmentRelationship rcr = folder.file(doc, AutoUniqueName.AUTO_UNIQUE, REPORT_NAME,
					DefineSecurityParentage.DO_NOT_DEFINE_SECURITY_PARENTAGE);
			rcr.save(RefreshMode.NO_REFRESH);

			// Delete the local file
			file.delete();
		} else {
			System.err.println("Report not found or already uploaded.");
		}

		reentrantLock.unlock();
	}

	private String getConvertedDate(String date) throws ParseException {

		Calendar calendar = Calendar.getInstance();
		Date dt = DATE_PATTERN.parse(date);
		calendar.setTime(dt);

		int numberMonth = calendar.get(Calendar.MONTH) + 1;
		String month = "";

		if (numberMonth < 10)
			month = "0" + numberMonth;
		else
			month = "" + numberMonth;

		return calendar.get(Calendar.YEAR) + "" + month + "" + calendar.get(Calendar.DATE) + ""
				+ DocumentPurgeConfigLoader.timezone_offset;
	}
}