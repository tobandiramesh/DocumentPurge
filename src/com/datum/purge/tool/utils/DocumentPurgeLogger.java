package com.datum.purge.tool.utils;

import org.apache.log4j.*;

/**
 * This class is used log the DocumentPurgeTool execution process.
 * 
 * @author DatumSolutions
 * @version 1.0
 *
 */

public class DocumentPurgeLogger {

	public static final int DEBUG = 1;
	public static final int INFO = 2;
	public static final int WARN = 3;
	public static final int ERROR = 4;
	public static final int FATAL = 5;

	public static Logger logger = Logger.getLogger(DocumentPurgeLogger.class);

	/**
	 * Write the message into the log file.
	 * 
	 * @param strClassName
	 * @param strMethodName
	 * @param iLogLevel     (DEBUG - 1 , INFO - 2 , WARN - 3, ERROR - 4, FATAL - 5)
	 * @param strMsg
	 */
	public static void writeLog(String strClassName, String strMethodName, int iLogLevel, String strMsg) {

		String strLoggerMessage = strClassName + " : " + strMethodName + " : " + strMsg;

		switch (iLogLevel) {
		case DocumentPurgeLogger.INFO:
			logger.info(strLoggerMessage);
			break;

		case DocumentPurgeLogger.DEBUG:
			logger.debug(strLoggerMessage);
			break;

		case DocumentPurgeLogger.ERROR:
			logger.error(strLoggerMessage);
			break;

		case DocumentPurgeLogger.FATAL:
			logger.fatal(strLoggerMessage);
			break;

		case DocumentPurgeLogger.WARN:
			logger.warn(strLoggerMessage);
			break;

		default:
			logger.info(strLoggerMessage);
			break;
		}

	}

	/**
	 * Write error messages with exception stack trace into the log file.
	 * 
	 * @param strClassName
	 * @param strMethodName
	 * @param strMsg
	 * @param objThrowable
	 */
	public static void writeErrorLog(String strClassName, String strMethodName, String strMsg, Throwable objThrowable) {
		String strLoggerMessage = strClassName + " : " + strMethodName + " : " + strMsg;
		logger.error(strLoggerMessage, objThrowable);
	}
}
