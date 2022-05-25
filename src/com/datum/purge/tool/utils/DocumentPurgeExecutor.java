package com.datum.purge.tool.utils;

import java.io.ByteArrayInputStream;
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


	/**
	 * This method is used to run the purge utility and uploads the report into ECM system.
	 */
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
					StringBuilder finalReport = new StringBuilder();

					finalReport.append("Document Class");
					finalReport.append(",");

					for (int reportHeaderCount = 0; reportHeaderCount < reportHeaders.length; reportHeaderCount++) {
						finalReport.append(reportHeaders[reportHeaderCount]);
						finalReport.append(",");
					}

					finalReport.append("No'of Docs");
					finalReport.append(",");
					finalReport.append("Status");
					finalReport.append("\n");
					finalReport.append(REPORT_DATA);

					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"Report has been generated. \n "+finalReport.toString());


					// Upload the generated report to FileNet
					if (reentrantLock.tryLock(0, TimeUnit.SECONDS)) {
						uploadReport(finalReport.toString(), objectStore);
						DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
								"DocumentPurgeTool execution has been completed.");
					}					
				}
			}
		} catch (Exception exception) {

			DocumentPurgeLogger.writeErrorLog(CLASS_NAME, methodName, "Exception Occured", exception);
			DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG, "\n Report data \n" + REPORT_DATA);

		}
	}

	/**
	 * This method is used to upload the report file to the configured folder path.
	 * @param finalReport
	 * @param objectStore
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private void uploadReport(String finalReport, ObjectStore objectStore) throws Exception {

		Document reportDocument = Factory.Document.createInstance(objectStore, DocumentPurgeConfigLoader.ce_upload_docClass);
		reportDocument.getProperties().putValue("DocumentTitle", REPORT_NAME);

		// Set content
		ContentElementList reportContentList = Factory.ContentElement.createList();
		ContentTransfer reportContent = Factory.ContentTransfer.createInstance();
		reportContent.setCaptureSource(new ByteArrayInputStream(finalReport.getBytes()));
		reportContent.set_RetrievalName(REPORT_NAME);
		reportContentList.add(reportContent);

		reportDocument.set_ContentElements(reportContentList);
		reportDocument.set_MimeType("text/plain");


		reportDocument.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
		reportDocument.save(RefreshMode.NO_REFRESH);


		String[] props = { "ID", "ClassDescription" };
		reportDocument.fetchProperties(props);

		DocumentPurgeLogger.writeLog(CLASS_NAME, "uploadReport", DocumentPurgeLogger.DEBUG, "Report Id is:"+ reportDocument.get_Id());

		// File the document into folder
		com.filenet.api.core.Folder folder = Factory.Folder.getInstance(objectStore, "Folder", fetchFolderPath(objectStore));
		ReferentialContainmentRelationship rcr = folder.file(reportDocument, AutoUniqueName.AUTO_UNIQUE, REPORT_NAME,DefineSecurityParentage.DO_NOT_DEFINE_SECURITY_PARENTAGE);
		rcr.save(RefreshMode.NO_REFRESH);


		reentrantLock.unlock();
	}

	/**
	 * This method is used to check the configured folder path, if path not exists, it creates folder with /Year/Month-Name.
	 * @param objectStore
	 * @return Folder path.
	 */
	private String fetchFolderPath(ObjectStore objectStore)
	{

		com.filenet.api.core.Folder folder = null;
		String report_upload_folderPath = DocumentPurgeConfigLoader.ce_upload_folderPath;
		
		try
		{
			folder = Factory.Folder.fetchInstance(objectStore, report_upload_folderPath, null);

		}catch(Exception exception)
		{
			Calendar calendar = Calendar.getInstance();
			int reportYear = calendar.get(Calendar.YEAR);
			String reportMonth =new SimpleDateFormat("MMMM").format(calendar.getTime());

			report_upload_folderPath = "/"+reportYear+"/"+reportMonth;			
			com.filenet.api.core.Folder yearFolder = null;

			try
			{
				yearFolder = Factory.Folder.fetchInstance(objectStore, objectStore.get_RootFolder()+""+reportYear, null);

			}catch(Exception yearFolderException)
			{
				yearFolder = Factory.Folder.createInstance(objectStore,null);
				yearFolder.set_Parent(objectStore.get_RootFolder());
				yearFolder.set_FolderName(""+reportYear);
				yearFolder.save(RefreshMode.NO_REFRESH);
			}
			try
			{
				folder = Factory.Folder.fetchInstance(objectStore, yearFolder.get_PathName()+reportMonth, null);

			}catch(Exception yearFolderException)
			{
				folder = Factory.Folder.createInstance(objectStore,null);
				folder.set_Parent(yearFolder);
				folder.set_FolderName(reportMonth);
				folder.save(RefreshMode.NO_REFRESH);
			}
		}

		return report_upload_folderPath;
	}

	/**
	 * This method is used to create required date string.
	 * @param date
	 * @return date in required format.
	 * @throws ParseException
	 */
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