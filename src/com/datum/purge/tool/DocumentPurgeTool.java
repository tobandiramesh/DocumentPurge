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

	public String className = "DocumentPurgeTool";
	public static BufferedReader csvDataReader = null;
	public static int rowTotalCount = 0;

	public void runDocumentPurgeTool() {


		String methodName = "runDocumentPurgeTool";

		try {

			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "Starting..");

			DocumentPurgeConfigLoader objPurgeConfigLoader = new DocumentPurgeConfigLoader();
			objPurgeConfigLoader.loadDocumentPurgeToolConfigurartion();
			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,
					"Tool Configured values are \n" + objPurgeConfigLoader.toString());

			csvDataReader = new BufferedReader(new FileReader(DocumentPurgeConfigLoader.tool_InputFilePath));  
	
			BufferedReader dataCountReader = new BufferedReader(new FileReader(DocumentPurgeConfigLoader.tool_InputFilePath));
			
			while(dataCountReader.readLine() != null) rowTotalCount++;
			
			dataCountReader.close();

			int noOfThreads = DocumentPurgeConfigLoader.tool_processThread;

			for (int count = 0; count < noOfThreads; count++) {
				DocumentPurgeExecutor documentPurgeExecutorThread = new DocumentPurgeExecutor();
				documentPurgeExecutorThread.start();
			}

		} catch (Exception exception) {
			DocumentPurgeLogger.writeErrorLog(className, methodName, "***Exception Occured***", exception);
		} finally {

		}
	}

	public static void main(String[] args) throws Exception {

		DocumentPurgeTool objDocumentPurgeTool = new DocumentPurgeTool();
		objDocumentPurgeTool.runDocumentPurgeTool();
	}


}
