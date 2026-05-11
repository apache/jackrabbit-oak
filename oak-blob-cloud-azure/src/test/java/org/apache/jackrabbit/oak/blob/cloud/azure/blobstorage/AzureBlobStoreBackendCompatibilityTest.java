/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import java.lang.reflect.Field;
import java.net.URI;

import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Compatibility tests for direct-download and upload cache configuration in
 * {@link AzureBlobStoreBackend}. The assertions are intentionally behavior-
 * based and do not depend on a specific cache library.
 */
public class AzureBlobStoreBackendCompatibilityTest {

    @Test
    public void setHttpDownloadURIExpirySecondsUpdatesField() throws Exception {
        AzureBlobStoreBackend backend = new AzureBlobStoreBackend();

        backend.setHttpDownloadURIExpirySeconds(3600);

        assertEquals(3600, getIntField(backend, "httpDownloadURIExpirySeconds"));
    }

    @Test
    public void setHttpUploadURIExpirySecondsUpdatesField() throws Exception {
        AzureBlobStoreBackend backend = new AzureBlobStoreBackend();

        backend.setHttpUploadURIExpirySeconds(1800);

        assertEquals(1800, getIntField(backend, "httpUploadURIExpirySeconds"));
    }

    @Test
    public void setHttpDownloadURICacheSizeCreatesAndDisablesCache() throws Exception {
        AzureBlobStoreBackend backend = new AzureBlobStoreBackend();
        backend.setHttpDownloadURIExpirySeconds(3600);

        backend.setHttpDownloadURICacheSize(100);
        assertNotNull(getField(backend, "httpDownloadURICache"));

        backend.setHttpDownloadURICacheSize(0);
        assertNull(getField(backend, "httpDownloadURICache"));
    }

    @Test
    public void createHttpDownloadURIReturnsNullWhenDisabled() {
        AzureBlobStoreBackend backend = new AzureBlobStoreBackend();

        URI downloadURI = backend.createHttpDownloadURI(
                new DataIdentifier("test"),
                DataRecordDownloadOptions.DEFAULT);

        assertNull(downloadURI);
    }

    @Test
    public void initiateHttpUploadReturnsNullWhenDisabled() {
        AzureBlobStoreBackend backend = new AzureBlobStoreBackend();

        assertNull(backend.initiateHttpUpload(1024, 1, DataRecordUploadOptions.DEFAULT));
    }

    private static int getIntField(AzureBlobStoreBackend backend, String fieldName) throws Exception {
        Field field = AzureBlobStoreBackend.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (int) field.get(backend);
    }

    private static Object getField(AzureBlobStoreBackend backend, String fieldName) throws Exception {
        Field field = AzureBlobStoreBackend.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(backend);
    }
}
