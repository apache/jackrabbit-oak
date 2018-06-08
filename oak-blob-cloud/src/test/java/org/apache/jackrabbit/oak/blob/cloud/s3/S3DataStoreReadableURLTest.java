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

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.AbstractURLReadableBlobStoreTest;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.URLReadableDataStore;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class S3DataStoreReadableURLTest extends AbstractURLReadableBlobStoreTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static S3DataStore dataStore;

    @BeforeClass
    public static void setupClass() throws Exception {
        dataStore = (S3DataStore) S3DataStoreUtils.getS3DataStore(
                "org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore",
                getProperties("s3.config",
                        "aws.properties",
                        ".aws"),
                homeDir.newFolder().getAbsolutePath()
        );
        dataStore.setURLReadableBinaryExpirySeconds(expirySeconds);
    }

    @Override
    protected URLReadableDataStore getDataStore() {
        return dataStore;
    }

    @Override
    protected DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException {
        return ((S3DataStore)ds).addRecord(in, new BlobOptions().setUpload(BlobOptions.UploadType.SYNCHRONOUS));
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((S3DataStore)ds).deleteRecord(identifier);
    }

    @Test
    public void testGetReadUrlHonorsSetExpiration() {
        URL url = dataStore.getReadURL(new DataIdentifier("testIdentifier"));
        String queryString = url.getQuery();
        String expiry = null;
        for (String pairs : queryString.split("&")) {
            String[] kv = pairs.split("=", 2);
            if ("X-Amz-Expires".equals(kv[0])) {
                expiry = kv[1];
                break;
            }
        }

        assertNotNull(expiry);

        // This test is not exact -
        // Sometimes expiry == expirySeconds, sometimes expiry == expirySeconds-1
        assertTrue( (expirySeconds-2) <= Integer.parseInt(expiry));
        assertTrue( (expirySeconds+2) >= Integer.parseInt(expiry));
    }
}
