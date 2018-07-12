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

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordDirectAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDirectUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDirectUploadException;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;


/**
 * Amazon S3 data store extending from {@link AbstractSharedCachingDataStore}.
 */
public class S3DataStore extends AbstractSharedCachingDataStore implements ConfigurableDataRecordDirectAccessProvider {

    protected Properties properties;

    private S3Backend s3Backend;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    private int minRecordLength = 16 * 1024;

    /**
     * The minimum size of a file in order to do multi-part upload.
     */
    static final long minPartSize = S3Backend.MIN_MULTIPART_UPLOAD_PART_SIZE;

    /**
     * The maximum size of a multi-part upload part (AWS limitation).
     */
    static final long maxPartSize = S3Backend.MAX_MULTIPART_UPLOAD_PART_SIZE;

    /**
     * The maximum allowed size of an upload that can be done via single-put upload.
     * Beyond this size, multi-part uploading is required.  AWS limitation.
     */
    static final long maxSinglePutUploadSize = S3Backend.MAX_SINGLE_PUT_UPLOAD_SIZE;

    /**
     * The maximum allowed size of a binary upload supported by this provider.
     */
    static final long maxBinaryUploadSize = S3Backend.MAX_BINARY_UPLOAD_SIZE;

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
    // ConfigurableDataRecordDirectAccessProvider implementation
    //
    @Override
    public void setHttpUploadURIExpirySeconds(int seconds) {
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

    @Override
    public DataRecordDirectUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
            throws IllegalArgumentException, DataRecordDirectUploadException {
        if (0L >= maxUploadSizeInBytes) {
            throw new IllegalArgumentException("maxUploadSizeInBytes must be > 0");
        }
        else if (0 == maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (-1 > maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (maxUploadSizeInBytes > maxSinglePutUploadSize &&
                maxNumberOfURIs == 1) {
            throw new IllegalArgumentException(
                    String.format("Cannot do single-put upload with file size %d", maxUploadSizeInBytes)
            );
        }
        else if (maxUploadSizeInBytes > maxBinaryUploadSize) {
            throw new IllegalArgumentException(
                    String.format("Cannot do upload with file size %d", maxUploadSizeInBytes)
            );
        }
        if (null == s3Backend) {
            throw new DataRecordDirectUploadException("Backend not initialized");
        }
        return s3Backend.initiateHttpUpload(maxUploadSizeInBytes, maxNumberOfURIs);
    }

    @Override
    public DataRecord completeHttpUpload(@Nonnull String uploadToken)
            throws IllegalArgumentException, DataRecordDirectUploadException, DataStoreException {
        if (Strings.isNullOrEmpty(uploadToken)) {
            throw new IllegalArgumentException("uploadToken required");
        }

        if (s3Backend != null) {
            return s3Backend.completeHttpUpload(uploadToken);
        }

        return null;
    }

    @Override
    public void setHttpDownloadURIExpirySeconds(int seconds) {
        if (s3Backend != null) {
            s3Backend.setHttpDownloadURIExpirySeconds(seconds);
        }
    }

    @Override
    public void setHttpDownloadURICacheSize(int maxSize) {
        if (s3Backend != null) {
            s3Backend.setHttpDownloadURICacheSize(maxSize);
        }
    }

    @Override
    public URI getDownloadURI(@Nonnull DataIdentifier identifier) {
        if (s3Backend == null) {
            return null;
        }
        return s3Backend.createHttpDownloadURI(identifier);
    }
}
