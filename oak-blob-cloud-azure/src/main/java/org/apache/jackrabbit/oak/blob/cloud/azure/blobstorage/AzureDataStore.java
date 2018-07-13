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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import java.net.URL;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.ConfigurableHttpDataRecordProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.HttpDataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.HttpUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.UnsupportedHttpUploadArgumentsException;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;

public class AzureDataStore extends AbstractSharedCachingDataStore implements ConfigurableHttpDataRecordProvider {
    private int minRecordLength = 16*1024;

    /**
     * The minimum size of a file in order to do multi-part upload.
     */
    static final long minPartSize = AzureBlobStoreBackend.MIN_MULTIPART_UPLOAD_PART_SIZE;

    /**
     * The maximum size of a multi-part upload part (Azure limitation).
     */
    static final long maxPartSize = AzureBlobStoreBackend.MAX_MULTIPART_UPLOAD_PART_SIZE;

    /**
     * The maximum allowed size of an upload that can be done via single-put upload.
     * Beyond this size, multi-part uploading is required.  Azure limitation.
     */
    static final long maxSinglePutUploadSize = AzureBlobStoreBackend.MAX_SINGLE_PUT_UPLOAD_SIZE;

    /**
     * The maximum allowed size of a binary upload supported by this provider.
     */
    static final long maxBinaryUploadSize = AzureBlobStoreBackend.MAX_BINARY_UPLOAD_SIZE;

    protected Properties properties;

    private AzureBlobStoreBackend azureBlobStoreBackend;

    @Override
    protected AbstractSharedBackend createBackend() {
        azureBlobStoreBackend = new AzureBlobStoreBackend();
        if (null != properties) {
            azureBlobStoreBackend.setProperties(properties);
        }
        return azureBlobStoreBackend;
    }

    public void setProperties(final Properties properties) {
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
    // HttpDataRecordProvider Implementation
    //
    @Override
    public void setHttpUploadURLExpirySeconds(int seconds) {
        if (null != azureBlobStoreBackend) {
            azureBlobStoreBackend.setHttpUploadURLExpirySeconds(seconds);
        }
    }

    @Override
    public void setBinaryTransferAccelerationEnabled(boolean enabled) {
        // NOOP - not a feature of Azure Blob Storage
    }

    @Nullable
    @Override
    public HttpDataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURLs)
            throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        if (0L >= maxUploadSizeInBytes) {
            throw new UnsupportedHttpUploadArgumentsException("maxUploadSizeInBytes must be > 0");
        }
        else if (0L == maxNumberOfURLs) {
            throw new UnsupportedHttpUploadArgumentsException("maxNumberOfURLs must be > 0");
        }
        else if (maxUploadSizeInBytes > maxSinglePutUploadSize &&
                maxNumberOfURLs == 1) {
            throw new UnsupportedHttpUploadArgumentsException(
                    String.format("Cannot do single-put upload with file size %d", maxUploadSizeInBytes)
            );
        }
        else if (maxUploadSizeInBytes > maxBinaryUploadSize) {
            throw new UnsupportedHttpUploadArgumentsException(
                    String.format("Cannot do upload with file size %d", maxUploadSizeInBytes)
            );
        }
        if (null == azureBlobStoreBackend) {
            throw new HttpUploadException("Backend not initialized");
        }
        return azureBlobStoreBackend.initiateHttpUpload(maxUploadSizeInBytes, maxNumberOfURLs);
    }

    @Nonnull
    @Override
    public DataRecord completeHttpUpload(String uploadToken) throws HttpUploadException, DataStoreException {
        if (Strings.isNullOrEmpty(uploadToken)) {
            throw new IllegalArgumentException("uploadToken required");
        }

        if (azureBlobStoreBackend != null) {
            return azureBlobStoreBackend.completeHttpUpload(uploadToken);
        }

        return null;
    }

    @Override
    public void setHttpDownloadURLExpirySeconds(int seconds) {
        if (null != azureBlobStoreBackend) {
            azureBlobStoreBackend.setHttpDownloadURLExpirySeconds(seconds);
        }
    }

    public void setHttpDownloadURLCacheSize(int maxSize) {
        azureBlobStoreBackend.setHttpDownloadURLCacheSize(maxSize);
    }

    @Nullable
    @Override
    public URL getDownloadURL(DataIdentifier identifier) {
        if (null != azureBlobStoreBackend) {
            return azureBlobStoreBackend.createHttpDownloadURL(identifier);
        }
        return null;
    }
}
