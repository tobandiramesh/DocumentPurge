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

	public String CLASS_NAME = "DocumentPurgeExecutor";
	public static int ROW_COUNT = 0;
	public static StringBuilder REPORT_DATA = new StringBuilder();
	SimpleDateFormat DATE_PATTERN = new SimpleDateFormat("MM/dd/yyyy");

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

					if (csvDataCount > 0) 
					{

						String ceSearchFieldDataType = searchFields[csvDataCount - 1];

						if(ceSearchFieldDataType.contains("|") && "date".equalsIgnoreCase(ceSearchFieldDataType.split("\\|")[1]))
						{
							searchSQLQuery.append(ceSearchFieldDataType.split("\\|")[0] +" "+fieldOperators[csvDataCount - 1] + " ");							
							searchSQLQuery.append(getConvertedDate(csvDataValues[csvDataCount])).append("");

						}else if(ceSearchFieldDataType.contains("|") && "number".equalsIgnoreCase(ceSearchFieldDataType.split("\\|")[1]))
						{
							searchSQLQuery.append(ceSearchFieldDataType.split("\\|")[0]+ " "+fieldOperators[csvDataCount - 1] + " ");								
							searchSQLQuery.append(csvDataValues[csvDataCount]).append("");

						}else
						{
							searchSQLQuery.append(ceSearchFieldDataType.split("\\|")[0]+ " "+fieldOperators[csvDataCount - 1] +" '");							
							searchSQLQuery.append(csvDataValues[csvDataCount]).append("'");
						}

						rowStatus.append(csvDataValues[csvDataCount]);
						rowStatus.append(",");

						if (csvDataCount != csvDataValues.length - 1)
							searchSQLQuery.append(" AND ");

					} else 
					{
						searchSQLQuery.append(csvDataValues[csvDataCount]);
						searchSQLQuery.append("] where ");

						rowStatus.append(csvDataValues[csvDataCount]);
						rowStatus.append(",");
					}
				}

				int iCSVDataValues = csvDataValues.length -1;
				if(iCSVDataValues < searchFields.length)
				{
					for (int i = 0; i < searchFields.length - iCSVDataValues; i++) {
						rowStatus.append(",");
					}
				}

				DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,"Executing Search Query : ["+searchSQLQuery.toString()+"]");
				SearchSQL searchSQL = new SearchSQL(searchSQLQuery.toString());
				SearchScope search = new SearchScope(objectStore);

				DocumentSet documentSet = (DocumentSet) search.fetchObjects(searchSQL, 100, null, true);

				if (!documentSet.isEmpty()) 
				{
					Iterator<?> it = documentSet.iterator();

					int docCount = 0;

					while (it.hasNext()) 
					{
						Document document = (Document) it.next();

						try 
						{
							document.fetchProperties(new String[] { PropertyNames.ID });
							String name = document.get_Name();
							String id = document.get_Id().toString();

							document.delete();
							document.save(RefreshMode.REFRESH);
							docCount++;
							DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
									"\n Document Name: " + name + " Document Id: " + id + " has been deleted.");

						} 
						catch (Exception exception) 
						{

							DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG, exception.getMessage());
						}
					}

					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"\n Total {" + docCount + "} Document(s) have been deleted for "+ csvDataLine);


					rowStatus.append(docCount);
					rowStatus.append(",");
					rowStatus.append(" document(s) deleted ");

				}else 
				{
					rowStatus.append(0);
					rowStatus.append(",");
					rowStatus.append(" No documents found ");

					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
							"\n No documents have been found for "+ csvDataLine);
				}
				ROW_COUNT++;

				rowStatus.append("\n");
				REPORT_DATA.append(rowStatus.toString());

				if (ROW_COUNT == DocumentPurgeTool.ROW_TOTAL_COUNT) {

					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG, "Report Data \n"+REPORT_DATA);
					FileWriter csvWriter = new FileWriter("DeletionReport.csv");
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
					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG, "Report has been generated.");
					DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,  "DocumentPurgeTool execution has been completed.");

				}
			}
		} catch (Exception exception) {

			DocumentPurgeLogger.writeErrorLog(CLASS_NAME, methodName, "Exception Occured", exception);
			DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG,
					"\n Report data \n" + REPORT_DATA);

		}
	}


	private String getConvertedDate(String date) throws ParseException {

		Calendar calendar = Calendar.getInstance();
		Date dt = DATE_PATTERN.parse(date);
		calendar.setTime(dt);

		int numberMonth = calendar.get(Calendar.MONTH) + 1;
		String month = "";

		if( numberMonth < 10 )
			month = "0"+numberMonth;
		else
			month = ""+numberMonth;		

		return calendar.get(Calendar.YEAR) + "" + month + "" + calendar.get(Calendar.DATE) + ""
		+ DocumentPurgeConfigLoader.timezone_offset;
	}
}