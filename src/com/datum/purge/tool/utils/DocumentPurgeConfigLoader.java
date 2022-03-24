package com.datum.purge.tool.utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class is used to load the DocumentPurgeTool configuration data.
 * @author DatumSolutions
 * @version 1.0
 *
 */

public class DocumentPurgeConfigLoader 
{


	public static String ce_UserID = "";	
	public static String ce_Password = "";	
	public static String ce_Domain = "";
	public static String ce_ObjectStoreName = "";
	public static String ce_DocumentClass = "";
	public static String ce_URI = "";
	public static String ce_SearchFields = "";
	public static String tool_InputFilePath = "";
	static String className = "DocumentPurgeToolConfigLoader";


	public void loadDocumentPurgeToolConfigurartion() throws Exception
	{
		String methodName = "loadDocumentPurgeToolConfigurartion";
		try
		{
			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "Starting...");

			Properties configuration = new Properties();
			InputStream input = new FileInputStream(DocumentPurgeConfig.TOOL_CONFIGURATION_FILENAME);
			configuration.load(input);

			ce_URI	 = 	configuration.getProperty(DocumentPurgeConfig.CE_URI).trim();
			ce_UserID 		 = 	configuration.getProperty(DocumentPurgeConfig.CE_USERNAME).trim();
			ce_Password 		 = 	configuration.getProperty(DocumentPurgeConfig.CE_PASSWORD).trim();
			ce_Domain 	 = 	configuration.getProperty(DocumentPurgeConfig.CE_DOMAIN).trim();
			ce_ObjectStoreName = 	configuration.getProperty(DocumentPurgeConfig.CE_OBJECTSTORE_NAME).trim();
			tool_InputFilePath  = 	configuration.getProperty(DocumentPurgeConfig.TOOL_INPUTFILE_PATH).trim();
			ce_DocumentClass  = 	configuration.getProperty(DocumentPurgeConfig.CE_DOCUMENTCLASS_NAME).trim();
			ce_SearchFields  = 	configuration.getProperty(DocumentPurgeConfig.CE_SEARCHFIELDS).trim();


		}		
		catch (Exception exception) 
		{
			DocumentPurgeLogger.writeErrorLog(className, methodName, "**LOAD CONFIGURATION HAS FAILED***", exception);

			throw exception;
		}
		finally
		{
			DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "LOAD CONFIGURATION HAS COMPLETED");
		}	
	}

	@Override
	public String toString() 
	{
		return " CE User["+ce_UserID+"] \n CE URI["+ce_URI+"]\n CE Document Class["+ce_DocumentClass+"]\n" +
				" CE DOCMAIN ["+ce_Domain+"]\n CE ObjectStore Name ["+ce_ObjectStoreName+"]\n Input File Path ["+tool_InputFilePath+"]\n Search Fields ["+ce_SearchFields+"]\n";
	}
}
