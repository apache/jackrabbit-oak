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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.AbstractURLReadableBlobStoreTest;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.URLReadableDataStore;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AzureDataStoreReadableURLTest extends AbstractURLReadableBlobStoreTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static AzureDataStore dataStore;

    @BeforeClass
    public static void setupClass() throws Exception {
        dataStore = new AzureDataStore();
        Map<String, String> propsAsMap = Maps.newHashMap();
        Properties props = getProperties(
                "azure.blob.config",
                "azure.properties",
                ".azure"
        );
        for (String key : props.stringPropertyNames()) {
            propsAsMap.put(key, props.getProperty(key));
        }
        PropertiesUtil.populate(dataStore, propsAsMap, false);
        dataStore.setProperties(props);
        dataStore.init(homeDir.newFolder().getAbsolutePath());
        dataStore.setURLReadableBinaryExpirySeconds(expirySeconds);
    }

    @Override
    protected URLReadableDataStore getDataStore() {
        return dataStore;
    }

    @Override
    protected DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException {
        return ((AzureDataStore)ds).addRecord(in, new BlobOptions().setUpload(BlobOptions.UploadType.SYNCHRONOUS));
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((AzureDataStore)ds).deleteRecord(identifier);
    }

    @Test
    public void testGetReadUrlHonorsSetExpiration() throws UnsupportedEncodingException {
        Instant expectedExpiry = Instant.now().plusSeconds(expirySeconds);
        URL url = dataStore.getReadURL(new DataIdentifier("testIdentifier"));
        String queryString = url.getQuery();
        String expiry = null;
        for (String pairs : queryString.split("&")) {
            String[] kv = pairs.split("=", 2);
            if ("se".equals(kv[0])) {
                expiry = kv[1];
                break;
            }
        }

        assertNotNull(expiry);

        String decodedExpiry = URLDecoder.decode(expiry, "utf-8");
        Instant actualExpiry = Instant.parse(decodedExpiry);
        assertTrue(expectedExpiry.minusSeconds(2).isBefore(actualExpiry));
        assertTrue(expectedExpiry.plusSeconds(2).isAfter(actualExpiry));
    }
}
