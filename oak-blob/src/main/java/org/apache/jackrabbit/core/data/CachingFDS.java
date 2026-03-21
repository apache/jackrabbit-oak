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

/**
 * {@link CachingDataStore} with {@link FSBackend}. It is performant 
 * {@link DataStore} when {@link FSBackend} is hosted on network storage 
 * (SAN or NAS). It leverages all caching capabilites of 
 * {@link CachingDataStore}.
 */
package org.apache.jackrabbit.core.data;

import java.util.Properties;

public class CachingFDS extends CachingDataStore {
    private Properties properties;

    @Override
    protected Backend createBackend() {
        FSBackend backend = new FSBackend();
        if (properties != null) {
            backend.setProperties(properties);
        }
        return backend;
    }

    @Override
    protected String getMarkerFile() {
        return "fs.init.done";
    }

    /**
     * Properties required to configure the S3Backend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
