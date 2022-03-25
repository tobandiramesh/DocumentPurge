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
import com.datum.purge.tool.utils.DocumentPurgeExecutor;
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
public class DocumentPurgeTool{

	public String className = "DocumentPurgeTool";
	public static Iterator<Row> rowIterator = null;
	public static int rowTotalCount = 0 ;

	public void runDocumentPurgeTool() {

		FileInputStream toolInputFile = null;
		XSSFWorkbook workbook = null;
		XSSFSheet sheet = null;

		String methodName = "runDocumentPurgeTool";

		try
		{

			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "Starting..");

			DocumentPurgeConfigLoader objPurgeConfigLoader = new DocumentPurgeConfigLoader();
			objPurgeConfigLoader.loadDocumentPurgeToolConfigurartion();
			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "Tool Configured values are \n"+objPurgeConfigLoader.toString());

			toolInputFile = new FileInputStream(new File(DocumentPurgeConfigLoader.tool_InputFilePath));
			workbook = new XSSFWorkbook(toolInputFile);
			sheet = workbook.getSheetAt(0);
			
			rowTotalCount = sheet.getLastRowNum();
			
			rowIterator = sheet.iterator();
			rowIterator.next();

			int noOfThreads = DocumentPurgeConfigLoader.tool_processThread;
			
			for (int count = 0; count < noOfThreads; count++) 
			{
				DocumentPurgeExecutor documentPurgeExecutorThread = new DocumentPurgeExecutor();
				documentPurgeExecutorThread.start();
			}			
			toolInputFile.close();
						
		} 
		catch (Exception exception) 
		{
			DocumentPurgeLogger.writeErrorLog(className, methodName, "***Exception Occured***", exception); 
		}finally
		{
			
			toolInputFile = null;
			workbook = null;
			sheet = null;
		}
	}

	public static void main(String[] args) throws Exception{

		DocumentPurgeTool objDocumentPurgeTool = new DocumentPurgeTool();
		objDocumentPurgeTool.runDocumentPurgeTool();
	}

}
