package com.datum.purge.tool.utils;

import java.io.FileWriter;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Row;

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
	public static int rowCount = 0 ;
	public static StringBuilder reportData = new StringBuilder();
	
	@Override
	public void run() {

		StringBuilder searchSQLQuery  = null;
		StringBuilder rowStatus = new StringBuilder();
		String methodName = "run";
		
		try
		{
			Iterator<Row> rowIterator = DocumentPurgeTool.rowIterator;
			ObjectStore objectStore = new DocumentPurgeUtil().fetchObjectStore();
			
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

				//searchSQLQuery.append(DocumentPurgeConfigLoader.ce_SearchFields.split(",")[1]+"='");
				//searchSQLQuery.append(customerNumber+"'");

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
					int docCount 	 = 0;
					while (it.hasNext()) 
					{
						Document document = (Document) it.next();
						document.fetchProperties(new String[]{PropertyNames.ID});

						docCount++;

						String name = document.get_Name();
						String id = document.get_Id().toString();

						document.delete();
						document.save(RefreshMode.REFRESH);

						DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "\n Document Name: " + name +" Document Id: "+id+" has been deleted for Policy Number ["+policyNumber+"] and Customer Number ["+customerNumber+"]");

					}

					DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "\n Total {"+docCount+"} Document(s) have been deleted for Policy Number ["+policyNumber+"] and Customer Number ["+customerNumber+"]");
					rowStatus.append(docCount);
					rowStatus.append(",");
					rowStatus.append(" document(s) deleted " );

				}else{

					DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "\n No Document(s) have been found for Policy Number ["+policyNumber+"] and Customer Number ["+customerNumber+"]");
					rowStatus.append(0);
					rowStatus.append(",");
					rowStatus.append(" No documents found " );
				}
				rowCount++;
				rowStatus.append("\n");
			}
			
			reportData.append(rowStatus.toString());

			if(rowCount == DocumentPurgeTool.rowTotalCount)
			{
				FileWriter csvWriter = new FileWriter(new Date().getTime()+"_DeletionReport.csv");
				csvWriter.append("Policy Number");
				csvWriter.append(",");
				csvWriter.append("Customer Number");
				csvWriter.append(",");
				csvWriter.append("No'of Docs");
				csvWriter.append(",");
				csvWriter.append("Status");
				csvWriter.append("\n"); 
				csvWriter.append(reportData);
				csvWriter.flush();
				csvWriter.close();
			}			
			
		}catch(Exception exception){
			
			DocumentPurgeLogger.writeErrorLog(className, methodName, "Exception Occured", exception);
			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "\n Report data \n"+reportData);

			
		}
	}

}
