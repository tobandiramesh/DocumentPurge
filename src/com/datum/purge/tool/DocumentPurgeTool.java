package com.datum.purge.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Date;
import java.util.Iterator;


import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


import com.datum.purge.tool.utils.DocumentPurgeConfigLoader;
import com.datum.purge.tool.utils.DocumentPurgeLogger;
import com.datum.purge.tool.utils.DocumentPurgeUtil;
import com.filenet.api.collection.DocumentSet;
import com.filenet.api.constants.PropertyNames;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Document;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import com.filenet.api.core.ObjectStore;

/**
 * This class is used to execute the DocumentPurgeTool.
 * @author DatumSolutions
 * @version 1.0
 *
 */
public class DocumentPurgeTool {

	public String className = "DocumentPurgeTool";

	public void runDocumentPurgeTool() throws Exception {

		ObjectStore objectStore = null;
		FileInputStream toolInputFile = null;
		XSSFWorkbook workbook = null;
		XSSFSheet sheet = null;
		Iterator<Row> rowIterator = null;
		StringBuilder searchSQLQuery  = null;
		FileWriter csvWriter = null;
		StringBuilder rowStatus = new StringBuilder();

		String methodName = "runDocumentPurgeTool";


		try
		{

			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "Starting..");

			if(objectStore == null)
			objectStore = new DocumentPurgeUtil().fetchObjectStore(); 

			toolInputFile = new FileInputStream(new File(DocumentPurgeConfigLoader.tool_InputFilePath));
			workbook = new XSSFWorkbook(toolInputFile);
			sheet = workbook.getSheetAt(0);

			rowIterator = sheet.iterator();
			rowIterator.next();

			while (rowIterator.hasNext()) 
			{
				Row row = rowIterator.next();
				String policyNumber = row.getCell(0).toString();
				String customerNumber = row.getCell(1).toString();

				searchSQLQuery  = 	new StringBuilder();

				searchSQLQuery.append("Select * from [");
				searchSQLQuery.append(DocumentPurgeConfigLoader.ce_DocumentClass);
				searchSQLQuery.append("] where ");
				searchSQLQuery.append(DocumentPurgeConfigLoader.ce_SearchFields.split(",")[0]+"='");
				searchSQLQuery.append(policyNumber).append("' AND ");
				searchSQLQuery.append(DocumentPurgeConfigLoader.ce_SearchFields.split(",")[1]+"=");
				searchSQLQuery.append(customerNumber);


				SearchSQL searchSQL = new SearchSQL(searchSQLQuery.toString());
				SearchScope search = new SearchScope(objectStore); 

				DocumentSet documentSet = (DocumentSet)search.fetchObjects(searchSQL, 100, null, true);


				rowStatus.append(policyNumber);
				rowStatus.append(",");
				rowStatus.append(customerNumber);
				rowStatus.append(",");


				if(!documentSet.isEmpty())
				{
					Iterator it = documentSet.iterator();
					int DocCount 	 = 0;
					while (it.hasNext()) 
					{
						Document document = (Document) it.next();
						document.fetchProperties(new String[]{PropertyNames.ID});
						
						DocCount++;
						
						String name = document.get_Name();
						String id = document.get_Id().toString();
						
						document.delete();
						document.save(RefreshMode.REFRESH);

						DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "\n Document Name: " + name +" Document Id: "+id+" has been deleted for Policy Number ["+policyNumber+"] and Customer Number ["+customerNumber+"]");

					}

					DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "\n Total {"+DocCount+"} Document(s) have been deleted for Policy Number ["+policyNumber+"] and Customer Number ["+customerNumber+"]");
					rowStatus.append(DocCount);
					rowStatus.append(",");
					rowStatus.append(" document(s) deleted " );

				}else{

					DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "\n No Document(s) have been found for Policy Number ["+policyNumber+"] and Customer Number ["+customerNumber+"]");
					rowStatus.append(0);
					rowStatus.append(",");
					rowStatus.append(" No documents found " );
				}
				rowStatus.append("\n");
			}


			csvWriter = new FileWriter(new Date().getTime()+"_DeletionReport.csv");
			csvWriter.append("Policy Number");
			csvWriter.append(",");
			csvWriter.append("Customer Number");
			csvWriter.append(",");
			csvWriter.append("No'of Docs");
			csvWriter.append(",");
			csvWriter.append("Status");
			csvWriter.append("\n"); 
			csvWriter.append(rowStatus);
			csvWriter.flush();
			csvWriter.close();
			toolInputFile.close();
		} 
		catch (Exception exception) 
		{
			DocumentPurgeLogger.writeErrorLog(className, methodName, "***Exception Occured***", exception); 
		}finally
		{
			objectStore = null;
			toolInputFile = null;
			workbook = null;
			sheet = null;
			
			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "DocumentPurgeTool execution is completed.");
		}
	}
	
	
	public static void main(String[] args) throws Exception{

		new DocumentPurgeTool().runDocumentPurgeTool();
	}

}
