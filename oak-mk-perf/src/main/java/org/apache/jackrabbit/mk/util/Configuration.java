package org.apache.jackrabbit.mk.util;

import java.util.Properties;

public class Configuration {

	    
	    private static final String MK_TYPE="mk.type";
	    private static final String HOST = "hostname";
	    private static final String MONGO_PORT = "mongo.port";
	    private static final String STORAGE_PATH = "storage.path";
	    private static final String DATABASE="mongo.database";
	    
	    private final Properties properties;

	    public Configuration(Properties properties) {
	        this.properties = properties;
	    }

	    public String getMkType() {
	        return properties.getProperty(MK_TYPE);
	    }

	    public String getHost() {
	        return properties.getProperty(HOST);
	    }

	    public int getMongoPort() {
	        return Integer.parseInt(properties.getProperty(MONGO_PORT));
	    }
	    
	    public String getStoragePath() {
	        return properties.getProperty(STORAGE_PATH);
	    }

		public String getMongoDatabase() {
			
			return properties.getProperty(DATABASE);
		}
}


