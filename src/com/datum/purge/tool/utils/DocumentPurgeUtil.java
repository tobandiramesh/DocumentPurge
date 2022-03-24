package com.datum.purge.tool.utils;


import javax.security.auth.Subject;
import com.filenet.api.core.Connection;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.util.UserContext;

/**
 * This class is used to fetch FileNet System connection.
 * @author DatumSolutions
 * @version 1.0
 *
 */
public class DocumentPurgeUtil {

	private String className = "DocumentPurgeUtil";
	
	private Domain fetchFileNetConnection() throws Exception
	{
		
		String methodName = "fetchFileNetConnection";
		Connection connection 	= 	null;
		Subject subject 	= 	null;
		UserContext userContext	= 	null;
		Domain domain = null;
		
		try
		{
		DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "Starting..");

		DocumentPurgeConfigLoader objPurgeConfigLoader = new DocumentPurgeConfigLoader();
		objPurgeConfigLoader.loadDocumentPurgeToolConfigurartion();
		DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG, "Tool Configured values are \n"+objPurgeConfigLoader.toString());

		connection 	= 	Factory.Connection.getConnection(DocumentPurgeConfigLoader.ce_URI);
		subject 	= 	UserContext.createSubject(connection, DocumentPurgeConfigLoader.ce_UserID, DocumentPurgeConfigLoader.ce_Password, null);
		userContext = 	UserContext.get();
		userContext.pushSubject(subject);	      
		domain = Factory.Domain.fetchInstance(connection, DocumentPurgeConfigLoader.ce_Domain, null); 

		DocumentPurgeLogger.writeLog(className, methodName, DocumentPurgeLogger.DEBUG,"FileNet Connection has been established.");

		}catch(Exception exception)
		{
			throw exception;
		}finally{
			connection 	= 	null;
			subject 	= 	null;
			userContext = null;
		}
		return domain;
	}
	
	public ObjectStore fetchObjectStore() throws Exception{
		
		return Factory.ObjectStore.fetchInstance(fetchFileNetConnection(), DocumentPurgeConfigLoader.ce_ObjectStoreName, null); 
	}
	
}
