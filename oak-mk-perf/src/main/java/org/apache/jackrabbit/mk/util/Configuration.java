/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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


