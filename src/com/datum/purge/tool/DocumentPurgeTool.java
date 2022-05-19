package com.datum.purge.tool;

import java.io.BufferedReader;
import java.io.FileReader;

import com.datum.purge.tool.utils.DocumentPurgeConfigLoader;
import com.datum.purge.tool.utils.DocumentPurgeExecutor;
import com.datum.purge.tool.utils.DocumentPurgeLogger;

/**
 * This class is used to execute the DocumentPurgeTool.
 * 
 * @author DatumSolutions
 * @version 1.0
 *
 */

public class DocumentPurgeTool {

	public String CLASS_NAME = "DocumentPurgeTool";
	public static BufferedReader CSV_DATA_READER = null;
	public static int ROW_TOTAL_COUNT = 0;

	public void runDocumentPurgeTool() {


		String methodName = "runDocumentPurgeTool";

		try {

			DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG, "Starting..");

			DocumentPurgeConfigLoader objPurgeConfigLoader = new DocumentPurgeConfigLoader();
			objPurgeConfigLoader.loadDocumentPurgeToolConfigurartion();
			DocumentPurgeLogger.writeLog(CLASS_NAME, methodName, DocumentPurgeLogger.DEBUG, "Tool Configured values are \n" + objPurgeConfigLoader.toString());

			CSV_DATA_READER = new BufferedReader(new FileReader(DocumentPurgeConfigLoader.tool_InputFilePath));  
	
			BufferedReader dataCountReader = new BufferedReader(new FileReader(DocumentPurgeConfigLoader.tool_InputFilePath));
			
			while(dataCountReader.readLine() != null) ROW_TOTAL_COUNT++;
			
			dataCountReader.close();

			int noOfThreads = DocumentPurgeConfigLoader.tool_processThread;

			for (int count = 0; count < noOfThreads; count++) {
				DocumentPurgeExecutor documentPurgeExecutorThread = new DocumentPurgeExecutor();
				documentPurgeExecutorThread.start();
			}

		} catch (Exception exception) {
			DocumentPurgeLogger.writeErrorLog(CLASS_NAME, methodName, "***Exception Occured***", exception);
		}
	}

	public static void main(String[] args) throws Exception {

		DocumentPurgeTool objDocumentPurgeTool = new DocumentPurgeTool();
		objDocumentPurgeTool.runDocumentPurgeTool();
	}


}
