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

import java.net.URI;
import java.util.Properties;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * Amazon S3 data store extending from {@link AbstractSharedCachingDataStore}.
 */
public class S3DataStore extends AbstractSharedCachingDataStore implements ConfigurableDataRecordAccessProvider {

    protected Properties properties;

    private S3Backend s3Backend;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    private int minRecordLength = 16 * 1024;

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

    //
    // ConfigurableDataRecordAccessProvider implementation
    //
    @Override
    public void setDirectUploadURIExpirySeconds(int seconds) {
        if (s3Backend != null) {
            s3Backend.setHttpUploadURIExpirySeconds(seconds);
        }
    }

    @Override
    public void setBinaryTransferAccelerationEnabled(boolean enabled) {
        if (s3Backend != null) {
            s3Backend.setBinaryTransferAccelerationEnabled(enabled);
        }
    }

    @Nullable
    @Override
    public DataRecordUpload initiateDataRecordUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
            throws IllegalArgumentException, DataRecordUploadException {
        if (null == s3Backend) {
            throw new DataRecordUploadException("Backend not initialized");
        }
        return s3Backend.initiateHttpUpload(maxUploadSizeInBytes, maxNumberOfURIs);
    }

    @NotNull
    @Override
    public DataRecord completeDataRecordUpload(@NotNull String uploadToken)
            throws IllegalArgumentException, DataRecordUploadException, DataStoreException {
        if (null == s3Backend) {
            throw new DataRecordUploadException("Backend not initialized");
        }
        return s3Backend.completeHttpUpload(uploadToken);
    }

    @Override
    public void setDirectDownloadURIExpirySeconds(int seconds) {
        if (s3Backend != null) {
            s3Backend.setHttpDownloadURIExpirySeconds(seconds);
        }
    }

    @Override
    public void setDirectDownloadURICacheSize(int maxSize) {
        if (s3Backend != null) {
            s3Backend.setHttpDownloadURICacheSize(maxSize);
        }
    }

    @Nullable
    @Override
    public URI getDownloadURI(@NotNull DataIdentifier identifier,
                              @NotNull DataRecordDownloadOptions downloadOptions) {
        if (s3Backend == null) {
            return null;
        }
        return s3Backend.createHttpDownloadURI(identifier, downloadOptions);
    }
}
