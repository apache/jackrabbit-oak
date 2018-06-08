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

package org.apache.jackrabbit.oak.spi.blob;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public abstract class AbstractURLReadableBlobStoreTest {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractURLReadableBlobStoreTest.class);

    protected static int expirySeconds = 60*15;
    protected static final String TEST_DATA = "Hi, I am blob test data";

    protected abstract URLReadableDataStore getDataStore();
    protected abstract DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException;
    protected abstract void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;

    protected static Properties getProperties(
            String systemPropertyName,
            String defaultPropertyFileName,
            String userHomePropertyDir) {
        File propertiesFile = new File(System.getProperty(systemPropertyName, defaultPropertyFileName));
        if (! propertiesFile.exists()) {
            propertiesFile = Paths.get(System.getProperty("user.home"), userHomePropertyDir, defaultPropertyFileName).toFile();
        }
        if (! propertiesFile.exists()) {
            propertiesFile = new File("./src/test/resources/" + defaultPropertyFileName);
        }
        Properties props = new Properties();
        try {
            props.load(new FileReader(propertiesFile));
        }
        catch (IOException e) {
            LOG.error("Couldn't load data store properties - try setting -D{}=<path>", systemPropertyName);
        }
        return props;
    }

    @Test
    public void testGetReadUrlProvidesValidUrl() {
        DataIdentifier id = new DataIdentifier("testIdentifier");
        URL url = getDataStore().getReadURL(id);
        assertNotNull(url);
    }

    @Test
    public void testGetReadUrlRequiresValidIdentifier() {
        try {
            getDataStore().getReadURL(null);
            fail();
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testGetReadUrlExpirationOfZeroFails() {
        URLReadableDataStore dataStore = getDataStore();
        try {
            dataStore.setURLReadableBinaryExpirySeconds(0);
            assertNull(dataStore.getReadURL(new DataIdentifier("testIdentifier")));
        }
        finally {
            dataStore.setURLReadableBinaryExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testGetReadUrlIT() throws DataStoreException, IOException {
        DataRecord record = null;
        URLReadableDataStore dataStore = getDataStore();
        try {
            record = doSynchronousAddRecord(dataStore, new ByteArrayInputStream(TEST_DATA.getBytes()));
            URL url = dataStore.getReadURL(record.getIdentifier());
            HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            assertEquals(200, conn.getResponseCode());

            StringWriter writer = new StringWriter();
            IOUtils.copy(conn.getInputStream(), writer, "utf-8");

            assertEquals(TEST_DATA, writer.toString());
        }
        finally {
            if (null != record) {
                doDeleteRecord(dataStore, record.getIdentifier());
            }
        }
    }

    @Test
    public void testGetExpiredReadUrlFailsIT() throws DataStoreException, IOException {
        DataRecord record = null;
        URLReadableDataStore dataStore = getDataStore();
        try {
            dataStore.setURLReadableBinaryExpirySeconds(2);
            record = doSynchronousAddRecord(dataStore, new ByteArrayInputStream(TEST_DATA.getBytes()));
            URL url = dataStore.getReadURL(record.getIdentifier());
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
            }
            HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            assertEquals(403, conn.getResponseCode());
        }
        finally {
            if (null != record) {
                doDeleteRecord(dataStore, record.getIdentifier());
            }
            dataStore.setURLReadableBinaryExpirySeconds(expirySeconds);
        }
    }
}
