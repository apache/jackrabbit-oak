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

import java.net.URI;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordDirectAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDirectUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDirectUploadException;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;

public class AzureDataStore extends AbstractSharedCachingDataStore implements ConfigurableDataRecordDirectAccessProvider {
    private int minRecordLength = 16*1024;

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
    // ConfigurableDataRecordDirectAccessProvider Implementation
    //
    @Override
    public void setDirectUploadURIExpirySeconds(int seconds) {
        if (null != azureBlobStoreBackend) {
            azureBlobStoreBackend.setHttpUploadURIExpirySeconds(seconds);
        }
    }

    @Override
    public void setBinaryTransferAccelerationEnabled(boolean enabled) {
        // NOOP - not a feature of Azure Blob Storage
    }

    @Nullable
    @Override
    public DataRecordDirectUpload initiateDirectUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
            throws IllegalArgumentException, DataRecordDirectUploadException {
        if (null == azureBlobStoreBackend) {
            throw new DataRecordDirectUploadException("Backend not initialized");
        }
        return azureBlobStoreBackend.initiateHttpUpload(maxUploadSizeInBytes, maxNumberOfURIs);
    }

    @Nonnull
    @Override
    public DataRecord completeDirectUpload(String uploadToken)
            throws IllegalArgumentException, DataRecordDirectUploadException, DataStoreException {
        if (null == azureBlobStoreBackend) {
            throw new DataRecordDirectUploadException("Backend not initialized");
        }
        return azureBlobStoreBackend.completeHttpUpload(uploadToken);
    }

    @Override
    public void setDirectDownloadURIExpirySeconds(int seconds) {
        if (null != azureBlobStoreBackend) {
            azureBlobStoreBackend.setHttpDownloadURIExpirySeconds(seconds);
        }
    }

    @Override
    public void setDirectDownloadURICacheSize(int maxSize) {
        azureBlobStoreBackend.setHttpDownloadURICacheSize(maxSize);
    }

    @Nullable
    @Override
    public URI getDownloadURI(@Nonnull DataIdentifier identifier) {
        return getDownloadURI(identifier, null);
    }

    @Nullable
    @Override
    public URI getDownloadURI(@Nonnull DataIdentifier identifier,
                              @Nullable Properties downloadOptions) {
        if (null != azureBlobStoreBackend) {
            return azureBlobStoreBackend.createHttpDownloadURI(identifier, downloadOptions);
        }
        return null;
    }
}
