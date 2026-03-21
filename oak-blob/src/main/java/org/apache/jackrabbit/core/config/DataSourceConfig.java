/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.core.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.naming.Context;

/**
 * This class contains list of definitions for {@code DataSource} instances.
 */
public class DataSourceConfig {

    public static final String DRIVER = "driver";

    public static final String URL = "url";

    public static final String USER = "user";

    public static final String PASSWORD = "password";

    public static final String DB_TYPE = "databaseType";

    public static final String VALIDATION_QUERY = "validationQuery";

    public static final String MAX_POOL_SIZE = "maxPoolSize";

    private final List<DataSourceDefinition> defs = new ArrayList<DataSourceDefinition>();

    /**
     * Adds a DataSourceDefinition from the given properties.
     * 
     * @param props the properties (key and values must be strings)
     * @throws ConfigurationException on error
     */
    public void addDataSourceDefinition(String name, Properties props) throws ConfigurationException {
        DataSourceDefinition def = new DataSourceDefinition(name, props);
        for (DataSourceDefinition existing : defs) {
            if (existing.getLogicalName().equals(def.getLogicalName())) {
                throw new ConfigurationException("Duplicate logicalName for a DataSource: "
                        + def.getLogicalName());
            }
        }
        defs.add(def);
    }

    /**
     * @return the unmodifiable list of the current {@link DataSourceDefinition}s
     */
    public List<DataSourceDefinition> getDefinitions() {
        return Collections.unmodifiableList(defs);
    }

    /**
     * The definition of a DataSource. 
     */
    public static final class DataSourceDefinition {

        private static final List<String> allPropNames =
            Arrays.asList(DRIVER, URL, USER, PASSWORD, DB_TYPE, VALIDATION_QUERY, MAX_POOL_SIZE);

        private static final List<String> allJndiPropNames =
            Arrays.asList(DRIVER, URL, USER, PASSWORD, DB_TYPE);

        private final String logicalName;

        private final String driver;

        private final String url;

        private final String user;

        private final String password;

        private final String dbType;

        private final String validationQuery;

        private final int maxPoolSize;

        /**
         * Creates a DataSourceDefinition from the given properties and 
         * throws a {@link ConfigurationException} when the set of properties does not
         * satisfy some validity constraints.
         * 
         * @param name the logical name of the data source
         * @param props the properties (string keys and values)
         * @throws ConfigurationException on error
         */
        public DataSourceDefinition(String name, Properties props) throws ConfigurationException {
            this.logicalName = name;
            this.driver = (String) props.getProperty(DRIVER);
            this.url = (String) props.getProperty(URL);
            this.user = (String) props.getProperty(USER);
            this.password = (String) props.getProperty(PASSWORD);
            this.dbType = (String) props.getProperty(DB_TYPE);
            this.validationQuery = (String) props.getProperty(VALIDATION_QUERY);
            try {
                this.maxPoolSize = Integer.parseInt((String) props.getProperty(MAX_POOL_SIZE, "-1"));
            } catch (NumberFormatException e) {
                throw new ConfigurationException("failed to parse " + MAX_POOL_SIZE
                        + " property for DataSource " + logicalName);
            }
            verify(props);
        }

        private void verify(Properties props) throws ConfigurationException {
            // Check required properties
            if (logicalName == null || "".equals(logicalName.trim())) {
                throw new ConfigurationException("DataSource logical name must not be null or empty");
            }
            if (driver == null || "".equals(driver)) {
                throw new ConfigurationException("DataSource driver must not be null or empty");
            }
            if (url == null || "".equals(url)) {
                throw new ConfigurationException("DataSource URL must not be null or empty");
            }
            if (dbType == null || "".equals(dbType)) {
                throw new ConfigurationException("DataSource databaseType must not be null or empty");
            }
            // Check unknown properties
            for (Object propName : props.keySet()) {
                if (!allPropNames.contains((String) propName)) {
                    throw new ConfigurationException("Unknown DataSource property: " + propName);
                }
            }
            // Check JNDI config:
            if (isJndiConfig()) {
                for (Object propName : props.keySet()) {
                    if (!allJndiPropNames.contains((String) propName)) {
                        throw new ConfigurationException("Property " + propName
                                + " is not allowed for a DataSource obtained through JNDI"
                                + ", DataSource logicalName = " + logicalName);
                    }
                }
            }
        }

        private boolean isJndiConfig() throws ConfigurationException {
            Class<?> driverClass = null;
            try {
                if (driver.length() > 0) {
                    driverClass = Class.forName(driver);
                }
            } catch (ClassNotFoundException e) {
                throw new ConfigurationException("Could not load JDBC driver class " + driver, e);
            }
            return driverClass != null && Context.class.isAssignableFrom(driverClass);
        }

        /**
         * @return the logicalName
         */
        public String getLogicalName() {
            return logicalName;
        }

        /**
         * @return the driver
         */
        public String getDriver() {
            return driver;
        }

        /**
         * @return the url
         */
        public String getUrl() {
            return url;
        }

        /**
         * @return the user
         */
        public String getUser() {
            return user;
        }

        /**
         * @return the dbType
         */
        public String getDbType() {
            return dbType;
        }

        /**
         * @return the password
         */
        public String getPassword() {
            return password;
        }

        /**
         * @return the validationQuery
         */
        public String getValidationQuery() {
            return validationQuery;
        }

        /**
         * @return the maxPoolSize
         */
        public int getMaxPoolSize() {
            return maxPoolSize;
        }
    }
}
