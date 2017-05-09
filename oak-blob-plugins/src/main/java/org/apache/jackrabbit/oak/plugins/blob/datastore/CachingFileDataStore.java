/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.util.Properties;

import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;

/**
 * File system implementation of {@link AbstractSharedCachingDataStore}.
 */
public class CachingFileDataStore extends AbstractSharedCachingDataStore {
    private Properties properties;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    private int minRecordLength = 16 * 1024;

    protected AbstractSharedBackend createBackend() {
        FSBackend
            backend = new FSBackend();
        if(this.properties != null) {
            backend.setProperties(this.properties);
        }

        return backend;
    }

    /*------------------------------------------- Getters & Setters-----------------------------**/

    /**
     * Properties required to configure the Backend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    protected AbstractSharedBackend getBackend() {
        return backend;
    }

    @Override
    public int getMinRecordLength() {
        return minRecordLength;
    }

    public void setMinRecordLength(int minRecordLength) {
        this.minRecordLength = minRecordLength;
    }
}
