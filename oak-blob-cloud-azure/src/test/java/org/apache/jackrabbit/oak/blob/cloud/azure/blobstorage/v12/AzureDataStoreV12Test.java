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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for AzureDataStoreV12 — covers guards and config delegation before init().
 */
public class AzureDataStoreV12Test {

    @Test
    public void getMinRecordLength_default_returns16k() {
        assertEquals(16 * 1024, new AzureDataStoreV12().getMinRecordLength());
    }

    @Test
    public void setMinRecordLength_updatesValue() {
        AzureDataStoreV12 store = new AzureDataStoreV12();
        store.setMinRecordLength(32 * 1024);
        assertEquals(32 * 1024, store.getMinRecordLength());
    }

    /**
     * initiateDataRecordUpload must throw, not NPE, when the backend was never initialized.
     */
    @Test(expected = DataRecordUploadException.class)
    public void initiateDataRecordUpload_beforeInit_throwsDataRecordUploadException()
            throws DataRecordUploadException {
        new AzureDataStoreV12().initiateDataRecordUpload(1024, 1);
    }

    /**
     * Same contract as the no-options overload — must throw, not NPE, when backend is null.
     */
    @Test(expected = DataRecordUploadException.class)
    public void initiateDataRecordUpload_withOptions_beforeInit_throwsDataRecordUploadException()
            throws DataRecordUploadException {
        new AzureDataStoreV12().initiateDataRecordUpload(1024, 1, DataRecordUploadOptions.DEFAULT);
    }

    /**
     * completeDataRecordUpload must throw, not NPE, when the backend was never initialized.
     */
    @Test(expected = DataRecordUploadException.class)
    public void completeDataRecordUpload_beforeInit_throwsDataRecordUploadException()
            throws DataRecordUploadException, DataStoreException {
        new AzureDataStoreV12().completeDataRecordUpload("some-token");
    }

    /**
     * getDownloadURI must return null, not NPE, when the backend was never initialized.
     */
    @Test
    public void getDownloadURI_beforeInit_returnsNull() {
        assertNull(new AzureDataStoreV12().getDownloadURI(
                new DataIdentifier("abc123"),
                org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions.DEFAULT));
    }

    /**
     * setDirectUploadURIExpirySeconds must be a no-op, not NPE, when backend is null.
     */
    @Test
    public void setDirectUploadURIExpirySeconds_beforeInit_doesNotThrow() {
        new AzureDataStoreV12().setDirectUploadURIExpirySeconds(300);
    }

    /**
     * setDirectDownloadURIExpirySeconds must be a no-op, not NPE, when backend is null.
     */
    @Test
    public void setDirectDownloadURIExpirySeconds_beforeInit_doesNotThrow() {
        new AzureDataStoreV12().setDirectDownloadURIExpirySeconds(300);
    }

    /**
     * setDirectDownloadURICacheSize must be a no-op, not NPE, when backend is null.
     */
    @Test
    public void setDirectDownloadURICacheSize_beforeInit_doesNotThrow() {
        new AzureDataStoreV12().setDirectDownloadURICacheSize(100);
    }
}
