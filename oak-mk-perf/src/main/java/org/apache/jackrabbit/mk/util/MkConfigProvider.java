package org.apache.jackrabbit.mk.util;

import java.io.InputStream;
import java.util.Properties;


public class MkConfigProvider {
	
	/**
	 * Read the mk configuration from file.
	 * @param resourcePath
	 * @return
	 * @throws Exception
	 */
	public static Configuration readConfig(String resourcePath) throws Exception {
		
		InputStream is = MkConfigProvider.class
				.getResourceAsStream(resourcePath);
		
		Properties properties = new Properties();
		properties.load(is);
		is.close();
		return new Configuration(properties);
	}
	/**
	 * Read the mk configuration from config.cfg.
	 * @param resourcePath
	 * @return
	 * @throws Exception
	 */
	public static Configuration readConfig() throws Exception {
		
		InputStream is = MkConfigProvider.class
				.getResourceAsStream("/config.cfg");
		
		Properties properties = new Properties();
		properties.load(is);
		System.out.println(properties.toString());
		is.close();
		/*
		Properties properties = new Properties();
		properties.setProperty("hostname", "localhost");
		properties.setProperty("mongo.port", "20017");
		properties.setProperty("mongo.database", "test");
		properties.setProperty("storage.path", "target/mk-tck-repo");
		*/
		return new Configuration(properties);
	}
	
}
