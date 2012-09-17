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
package org.apache.jackrabbit.mongomk.perf;

import java.util.Properties;

/**
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class Config {

    private static final String MASTER_HOST = "master.host";
    private static final String MASTER_PORT = "master.port";
    private static final String MONGO_DATABASE = "mongo.db";
    private static final String MONGO_HOST = "mongo.host";
    private static final String MONGO_PORT = "mongo.port";
    private final Properties properties;

    public Config(Properties properties) {
        this.properties = properties;
    }

    public String getMasterHost() {
        return properties.getProperty(MASTER_HOST);
    }

    public int getMasterPort() {
        return Integer.parseInt(properties.getProperty(MASTER_PORT));
    }

    public String getMongoDatabase() {
        return properties.getProperty(MONGO_DATABASE);
    }

    public String getMongoHost() {
        return properties.getProperty(MONGO_HOST);
    }

    public int getMongoPort() {
        return Integer.parseInt(properties.getProperty(MONGO_PORT));
    }
}
