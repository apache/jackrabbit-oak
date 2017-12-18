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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.CompositeDataStoreAware;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;

import java.util.Properties;


/**
 * Amazon S3 data store extending from {@link AbstractSharedCachingDataStore}.
 */
public class S3DataStore extends AbstractSharedCachingDataStore implements CompositeDataStoreAware {

    private boolean isDelegate = false;

    protected Properties properties;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    private int minRecordLength = 16 * 1024;

    @Override
    protected AbstractSharedBackend createBackend() {
        S3Backend backend = new S3Backend();
        if(properties != null){
            backend.setProperties(properties);
        }
        return backend;
    }

    /**------------------------------------------- Getters & Setters-----------------------------**/

    /**
     * Properties required to configure the S3Backend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public SharedBackend getBackend() {
        return backend;
    }

    @Override
    public int getMinRecordLength() {
        return minRecordLength;
    }

    public void setMinRecordLength(int minRecordLength) {
        this.minRecordLength = minRecordLength;
    }

    @Override
    public boolean isDelegate() {
        return isDelegate;
    }

    @Override
    public void setIsDelegate(boolean isDelegate) {
        this.isDelegate = isDelegate;
    }
}
