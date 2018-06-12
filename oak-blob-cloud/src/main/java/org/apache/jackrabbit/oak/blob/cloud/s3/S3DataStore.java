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

import com.google.common.base.Strings;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.DirectBinaryAccessException;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.apache.jackrabbit.oak.spi.blob.URLReadableDataStore;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStore;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStoreUploadContext;

import javax.annotation.Nonnull;
import java.net.URL;
import java.util.Properties;


/**
 * Amazon S3 data store extending from {@link AbstractSharedCachingDataStore}.
 */
public class S3DataStore extends AbstractSharedCachingDataStore implements URLWritableDataStore, URLReadableDataStore {

    protected Properties properties;

    private S3Backend s3Backend;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    private int minRecordLength = 16 * 1024;

    /**
     * The minimum size of a file in order to do multi-part upload.
     */
    static final int minPartSize = ((1024 * 1024 * 1024)/100) + 1; // 10MB

    @Override
    protected AbstractSharedBackend createBackend() {
        s3Backend = new S3Backend();
        if(properties != null){
            s3Backend.setProperties(properties);
        }
        return s3Backend;
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
    public void setURLWritableBinaryExpirySeconds(int seconds) {
        if (s3Backend != null) {
            s3Backend.setURLWritableBinaryExpirySeconds(seconds);
        }
    }

    @Override
    public void setURLBinaryTransferAcceleration(boolean enabled) {
        if (s3Backend != null) {
            s3Backend.setURLBinaryTransferAcceleration(enabled);
        }
    }

    @Override
    public URLWritableDataStoreUploadContext initDirectUpload(long maxUploadSizeInBytes, int maxNumberOfURLs)
            throws DirectBinaryAccessException {
        if (0L >= maxUploadSizeInBytes) {
            throw new DirectBinaryAccessException("maxUploadSizeInBytes must be > 0");
        }
        else if (0L >= maxNumberOfURLs) {
            throw new DirectBinaryAccessException("maxNumberOfURLs must be > 0");
        }
        if (s3Backend != null) {
            return s3Backend.initDirectUpload(maxUploadSizeInBytes, maxNumberOfURLs);
        }
        return null;
    }

    @Override
    public DataRecord completeDirectUpload(@Nonnull String uploadToken)
            throws DirectBinaryAccessException, DataStoreException {
        if (Strings.isNullOrEmpty(uploadToken)) {
            throw new IllegalArgumentException("uploadToken required");
        }

        if (s3Backend != null) {
            return s3Backend.completeDirectUpload(uploadToken);
        }

        return null;
    }

    @Override
    public void setURLReadableBinaryExpirySeconds(int seconds) {
        if (s3Backend != null) {
            s3Backend.setURLReadableBinaryExpirySeconds(seconds);
        }
    }

    @Override
    public void setURLReadableBinaryURLCacheSize(int maxSize) {
        if (s3Backend != null) {
            s3Backend.setURLReadableBinaryURLCacheSize(maxSize);
        }
    }

    @Override
    public URL getReadURL(@Nonnull DataIdentifier identifier) {
        if (s3Backend == null) {
            return null;
        }
        return s3Backend.createPresignedGetURL(identifier);
    }
}
