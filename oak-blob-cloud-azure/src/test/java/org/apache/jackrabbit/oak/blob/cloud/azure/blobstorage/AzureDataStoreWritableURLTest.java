/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.AbstractURLWritableBlobStoreTest;
import org.apache.jackrabbit.oak.spi.blob.DirectBinaryAccessException;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStore;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStoreUploadContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.net.ssl.HttpsURLConnection;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class AzureDataStoreWritableURLTest extends AbstractURLWritableBlobStoreTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static AzureDataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
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
        dataStore.setURLWritableBinaryExpirySeconds(expirySeconds);
    }

    @Override
    protected URLWritableDataStore getDataStore() {
        return dataStore;
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((AzureDataStore)ds).deleteRecord(identifier);
    }

    @Override
    protected long getProviderMinPartSize() {
        return Math.max(0L, AzureDataStore.minPartSize);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return AzureDataStore.maxPartSize;
    }

    @Override
    protected boolean isSinglePutURL(URL url) {
        Map<String, String> queryParams = parseQueryString(url);
        if (queryParams.containsKey("comp") || queryParams.containsKey("blockId")) {
            return false;
        }
        return true;
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URL url) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("x-ms-date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));
        conn.setRequestProperty("x-ms-version", "2017-11-09");

        Map<String, String> queryParams = parseQueryString(url);
        if (! queryParams.containsKey("comp") && ! queryParams.containsKey("blockId")) {
            // single put
            conn.setRequestProperty("x-ms-blob-type", "BlockBlob");
        }

        return conn;
    }

    @Test
    public void testInitDirectUploadURLHonorsExpiryTime() throws DirectBinaryAccessException {
        URLWritableDataStore ds = getDataStore();
        try {
            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            ds.setURLWritableBinaryExpirySeconds(60);
            URLWritableDataStoreUploadContext context = ds.initDirectUpload(ONE_MB, 1);
            URL uploadUrl = context.getUploadPartURLs().get(0);
            Map<String, String> params = parseQueryString(uploadUrl);
            String expiryDateStr = params.get("se");
            Instant expiry = Instant.parse(expiryDateStr);
            assertEquals(now, expiry.minusSeconds(60));
        }
        finally {
            ds.setURLWritableBinaryExpirySeconds(expirySeconds);
        }
    }
}
