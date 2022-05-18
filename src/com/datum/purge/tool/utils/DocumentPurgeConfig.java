package com.datum.purge.tool.utils;

/**
 * This class is used to define constants for all the configuration key's.
 * 
 * @author DatumSolutions
 * @version 1.0
 *
 */
class DocumentPurgeConfig {
	final static String CE_URI = "Document.Purge.System.URI";
	final static String CE_USERNAME = "Document.Purge.System.User";
	final static String CE_PASSWORD = "Document.Purge.System.Password";
	final static String CE_DOMAIN = "Document.Purge.System.Domain";

	final static String CE_OBJECTSTORE_NAME = "Document.Purge.System.ObjectStore";
	final static String CE_SEARCHFIELDS = "Document.Purge.System.SearchFields.SymbolicNames";
	final static String CE_FIELD_OPERATORS = "Document.Purge.System.SearchFields.Operators";
	final static String TIMEZONE_OFFSET = "Document.Purge.System.SearchFields.TimeZoneOffset";
	final static String TOOL_INPUTFILE_PATH = "Document.Purge.System.SearchData.FilePath";

	final static String TOOL_PROCESS_THREADS = "Document.Purge.System.Thread.Count";

	final static String TOOL_CONFIGURATION_FILENAME = "Document-Purge-Tool.properties";
}