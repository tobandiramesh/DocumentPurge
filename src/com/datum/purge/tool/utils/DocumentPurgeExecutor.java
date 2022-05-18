package com.datum.purge.tool.utils;

import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import com.datum.purge.tool.DocumentPurgeTool;
import com.filenet.api.collection.DocumentSet;
import com.filenet.api.constants.PropertyNames;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Document;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;

public class DocumentPurgeExecutor extends Thread {

	public String className = "DocumentPurgeTool";
	public static int rowCount = 0;
	public static StringBuilder reportData = new StringBuilder();
	SimpleDateFormat pattern = new SimpleDateFormat("MM/dd/YYYY");

	@Override
	public void run() {

		StringBuilder searchSQLQuery = null;
		StringBuilder rowStatus = new StringBuilder();
		String methodName = "run";
		String csvDataLine = "";
		String splitDelimeter = ",";

		try {
			ObjectStore objectStore = new DocumentPurgeUtil().fetchObjectStore();

			while ((csvDataLine = DocumentPurgeTool.csvDataReader.readLine()) != null) {
				String[] fieldOperators = DocumentPurgeConfigLoader.ce_field_operators.split(splitDelimeter);
				String[] csvDataValues = csvDataLine.split(splitDelimeter);

				searchSQLQuery = new StringBuilder();
				searchSQLQuery.append("Select * from [");
				for (int i = 0; i < csvDataValues.length; i++) {
					if (i > 0) {
						searchSQLQuery.append(DocumentPurgeConfigLoader.ce_SearchFields.split(",")[i - 1]
								+ fieldOperators[i - 1] + " ");
						searchSQLQuery.append(getConvertedDate(csvDataValues[i])).append("");
						rowStatus.append(csvDataValues[i]);
						rowStatus.append(",");
						if (i != csvDataValues.length - 1)
							searchSQLQuery.append(" AND ");
					} else {
						searchSQLQuery.append(csvDataValues[i]);
						searchSQLQuery.append("] where ");

						rowStatus.append(csvDataValues[i]);
						rowStatus.append(",");
					}
				}

				SearchSQL searchSQL = new SearchSQL(searchSQLQuery.toString());
				SearchScope search = new SearchScope(objectStore);

				System.out.println("searchSQL: " + searchSQL);

				DocumentSet documentSet = (DocumentSet) search.fetchObjects(searchSQL, 100, null, true);

				if (!documentSet.isEmpty()) {
					Iterator<?> it = documentSet.iterator();
					int docCount = 0;
					while (it.hasNext()) {
						Document document = (Document) it.next();
						document.fetchProperties(new String[] { PropertyNames.ID });

						docCount++;

						String name = document.get_Name();
						String id = document.get_Id().toString();
						try {
							document.delete();
							document.save(RefreshMode.REFRESH);

							DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,
									"\n Document Name: " + name + " Document Id: " + id + " has been deleted");

						} catch (Exception exception) {

							DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,
									exception.getMessage());
						}
					}

					DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,
							"\n Total {" + docCount + "} Document(s) have been deleted");

					rowStatus.append(",");
					rowStatus.append(docCount);
					rowStatus.append(",");
					rowStatus.append(" document(s) deleted ");

				} else {
					rowStatus.append(",");
					rowStatus.append(0);
					rowStatus.append(",");
					rowStatus.append(" No documents found ");
				}
				rowCount++;
				rowStatus.append("\n");

				reportData.append(rowStatus.toString());

				if (rowCount == DocumentPurgeTool.rowTotalCount) {
					FileWriter csvWriter = new FileWriter("DeletionReport.csv");
					csvWriter.append("Document Class");
					csvWriter.append(",");
					csvWriter.append("From Date");
					csvWriter.append(",");
					csvWriter.append("Document Date");
					csvWriter.append(",");
					csvWriter.append("No'of Docs");
					csvWriter.append(",");
					csvWriter.append("Status");
					csvWriter.append("\n");
					csvWriter.append(reportData);
					csvWriter.flush();
					csvWriter.close();
					DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,
							"Report has been generated.");
					DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,
							"DocumentPurgeTool execution has been completed.");

				}
			}
		} catch (Exception exception) {

			DocumentPurgeLogger.writeErrorLog(className, methodName, "Exception Occured", exception);
			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,
					"\n Report data \n" + reportData);

		}
	}

	private String getConvertedDate(String date) throws ParseException {
		Calendar c = Calendar.getInstance();
		Date dt = pattern.parse(date);
		c.setTime(dt);
		return c.get(Calendar.YEAR) + "" + c.get(Calendar.MONTH) + "" + c.get(Calendar.DATE) + ""
				+ DocumentPurgeConfigLoader.timezone_offset;
	}
}