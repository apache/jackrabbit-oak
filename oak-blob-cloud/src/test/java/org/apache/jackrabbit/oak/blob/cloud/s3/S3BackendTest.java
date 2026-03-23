/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Compatibility tests for direct-download and upload cache configuration in
 * {@link S3Backend}. The assertions are intentionally behavior-based and do
 * not reference third-party cache types.
 */
public class S3BackendTest {

    @Test
    public void setHttpDownloadURIExpirySecondsUpdatesField() throws Exception {
        S3Backend backend = new S3Backend();

        backend.setHttpDownloadURIExpirySeconds(3600);

        assertEquals(3600, getIntField(backend, "httpDownloadURIExpirySeconds"));
    }

    @Test
    public void setHttpUploadURIExpirySecondsUpdatesField() throws Exception {
        S3Backend backend = new S3Backend();

        backend.setHttpUploadURIExpirySeconds(1800);

        assertEquals(1800, getIntField(backend, "httpUploadURIExpirySeconds"));
    }

    @Test
    public void setHttpDownloadURICacheSizeCreatesAndDisablesCache() throws Exception {
        S3Backend backend = new S3Backend();
        backend.setHttpDownloadURIExpirySeconds(3600);

        backend.setHttpDownloadURICacheSize(100);
        assertNotNull(getField(backend, "httpDownloadURICache"));

        backend.setHttpDownloadURICacheSize(0);
        assertNull(getField(backend, "httpDownloadURICache"));
    }

    @Test
    public void createHttpDownloadURIReturnsNullWhenDisabled() {
        S3Backend backend = new S3Backend();

        URI downloadURI = backend.createHttpDownloadURI(
                new DataIdentifier("test"),
                DataRecordDownloadOptions.DEFAULT);

        assertNull(downloadURI);
    }

    @Test
    public void initiateHttpUploadReturnsNullWhenDisabled() {
        S3Backend backend = new S3Backend();

        assertNull(backend.initiateHttpUpload(1024, 1));
    }

    @Test
    public void createHttpDownloadURIReturnsCachedURIWithoutRecheckingStore() throws Exception {
        CacheHitBackend backend = new CacheHitBackend();
        DataIdentifier identifier = new DataIdentifier("cached");
        URI cachedUri = URI.create("https://cached.example/download");

        backend.setHttpDownloadURIExpirySeconds(300);
        backend.setHttpDownloadURICacheSize(10);
        putIntoCache(getField(backend, "httpDownloadURICache"), identifier, cachedUri);

        assertEquals(cachedUri, backend.createHttpDownloadURI(identifier, DataRecordDownloadOptions.DEFAULT));
    }

    private static int getIntField(S3Backend backend, String fieldName) throws Exception {
        Field field = S3Backend.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (int) field.get(backend);
    }

    private static Object getField(S3Backend backend, String fieldName) throws Exception {
        Field field = S3Backend.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(backend);
    }

    private static void putIntoCache(Object cache, Object key, Object value) throws Exception {
        Method put = cache.getClass().getMethod("put", Object.class, Object.class);
        put.setAccessible(true);
        put.invoke(cache, key, value);
    }

    private static final class CacheHitBackend extends S3Backend {
        @Override
        public boolean exists(DataIdentifier identifier) throws DataStoreException {
            throw new AssertionError("cached download URI should be returned before checking blob existence");
        }
    }
}
