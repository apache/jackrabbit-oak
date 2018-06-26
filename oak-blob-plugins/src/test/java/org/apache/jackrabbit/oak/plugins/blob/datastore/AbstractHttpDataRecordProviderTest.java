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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.net.ssl.HttpsURLConnection;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.util.Base64;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHttpDataRecordProviderTest {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHttpDataRecordProviderTest.class);

    protected abstract ConfigurableHttpDataRecordProvider getDataStore();
    protected abstract long getProviderMinPartSize();
    protected abstract long getProviderMaxPartSize();
    protected abstract long getProviderMaxSinglePutSize();
    protected abstract long getProviderMaxBinaryUploadSize();
    protected abstract boolean isSinglePutURI(URI uri);
    protected abstract HttpsURLConnection getHttpsConnection(long length, URI uri) throws IOException;
    protected abstract DataRecord doGetRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;
    protected abstract DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException;
    protected abstract void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;
    protected abstract boolean integrationTestsEnabled();

    protected static int expirySeconds = 60*15;

    protected static long ONE_KB = 1024;
    protected static long ONE_MB = ONE_KB * ONE_KB;

    protected static long TEN_MB = ONE_MB * 10;
    protected static long TWENTY_MB = ONE_MB * 20;
    protected static long ONE_HUNDRED_MB = ONE_MB * 100;
    protected static long FIVE_HUNDRED_MB = ONE_HUNDRED_MB * 5;
    protected static long ONE_GB = ONE_HUNDRED_MB * 10;
    protected static long FIVE_GB = ONE_GB * 5;

    protected static Properties getProperties(
            String systemPropertyName,
            String defaultPropertyFileName,
            String userHomePropertyDir) {
        File propertiesFile = new File(getProperty(systemPropertyName, defaultPropertyFileName));
        if (! propertiesFile.exists()) {
            propertiesFile = Paths.get(getProperty("user.home"), userHomePropertyDir, defaultPropertyFileName).toFile();
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

    //
    // Direct HTTP download tests
    //
    @Test
    public void testGetReadURIProvidesValidURI() {
        DataIdentifier id = new DataIdentifier("testIdentifier");
        URI uri = getDataStore().getDownloadURI(id);
        assertNotNull(uri);
    }

    @Test
    public void testGetReadURIRequiresValidIdentifier() {
        try {
            getDataStore().getDownloadURI(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testGetReadURIExpirationOfZeroFails() {
        ConfigurableHttpDataRecordProvider dataStore = getDataStore();
        try {
            dataStore.setHttpDownloadURIExpirySeconds(0);
            assertNull(dataStore.getDownloadURI(new DataIdentifier("testIdentifier")));
        }
        finally {
            dataStore.setHttpDownloadURIExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testGetReadURIIT() throws DataStoreException, IOException {
        DataRecord record = null;
        HttpDataRecordProvider dataStore = getDataStore();
        try {
            String testData = randomString(256);
            record = doSynchronousAddRecord((DataStore) dataStore,
                    new ByteArrayInputStream(testData.getBytes()));
            URI uri = dataStore.getDownloadURI(record.getIdentifier());
            HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("GET");
            assertEquals(200, conn.getResponseCode());

            StringWriter writer = new StringWriter();
            IOUtils.copy(conn.getInputStream(), writer, "utf-8");

            assertEquals(testData, writer.toString());
        }
        finally {
            if (null != record) {
                doDeleteRecord((DataStore) dataStore, record.getIdentifier());
            }
        }
    }

    @Test
    public void testGetExpiredReadURIFailsIT() throws DataStoreException, IOException {
        DataRecord record = null;
        ConfigurableHttpDataRecordProvider dataStore = getDataStore();
        try {
            String testData = randomString(256);
            dataStore.setHttpDownloadURIExpirySeconds(2);
            record = doSynchronousAddRecord((DataStore) dataStore,
                    new ByteArrayInputStream(testData.getBytes()));
            URI uri = dataStore.getDownloadURI(record.getIdentifier());
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
            }
            HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("GET");
            assertEquals(403, conn.getResponseCode());
        }
        finally {
            if (null != record) {
                doDeleteRecord((DataStore) dataStore, record.getIdentifier());
            }
            dataStore.setHttpDownloadURIExpirySeconds(expirySeconds);
        }
    }

    //
    // Direct HTTP upload tests
    //
    @Test
    public void testInitiateHttpUploadReturnsValidUploadContext() throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        HttpDataRecordUpload uploadContext =
                getDataStore().initiateHttpUpload(ONE_MB, 10);
        assertNotNull(uploadContext);
        assertFalse(uploadContext.getUploadURIs().isEmpty());
        assertTrue(uploadContext.getMinPartSize() > 0);
        assertTrue(uploadContext.getMinPartSize() >= getProviderMinPartSize());
        assertTrue(uploadContext.getMaxPartSize() >= uploadContext.getMinPartSize());
        assertTrue(uploadContext.getMaxPartSize() <= getProviderMaxPartSize());
        assertTrue((uploadContext.getMaxPartSize() * uploadContext.getUploadURIs().size()) >= ONE_MB);
        assertFalse(Strings.isNullOrEmpty(uploadContext.getUploadToken()));
    }

    @Test
    public void testInitiateHttpUploadRequiresNonzeroFileSize() throws HttpUploadException {
        try {
            getDataStore().initiateHttpUpload(0, 10);
            fail();
        }
        catch (UnsupportedHttpUploadArgumentsException e) { }
    }

    @Test
    public void testInititateHttpUploadRequiresNonzeroNumURIs() throws HttpUploadException {
        try {
            getDataStore().initiateHttpUpload(ONE_MB, 0);
            fail();
        }
        catch (UnsupportedHttpUploadArgumentsException e) { }
    }

    @Test
    public void testInititateHttpUploadSingleURLRequested() throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        HttpDataRecordUpload uploadContext =
                getDataStore().initiateHttpUpload(TWENTY_MB, 1);
        assertEquals(1, uploadContext.getUploadURIs().size());
        assertTrue(isSinglePutURI(uploadContext.getUploadURIs().iterator().next()));
    }

    @Test
    public void testInititateHttpUploadSizeLowerThanMinPartSize() throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        HttpDataRecordUpload uploadContext =
                getDataStore().initiateHttpUpload(getProviderMinPartSize()-1L, 10);
        assertEquals(1, uploadContext.getUploadURIs().size());
        assertTrue(isSinglePutURI(uploadContext.getUploadURIs().iterator().next()));
    }

    @Test
    public void testInititateHttpUploadMultiPartDisabled() throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        ConfigurableHttpDataRecordProvider ds = getDataStore();
        try {
            ds.setHttpUploadURIExpirySeconds(0);
            HttpDataRecordUpload uploadContext = ds.initiateHttpUpload(TWENTY_MB, 10);
            assertEquals(0, uploadContext.getUploadURIs().size());

            uploadContext = ds.initiateHttpUpload(20, 1);
            assertEquals(0, uploadContext.getUploadURIs().size());
        }
        finally {
            ds.setHttpUploadURIExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInititateHttpUploadURIListSizes() throws UnsupportedHttpUploadArgumentsException, HttpUploadException {
        HttpDataRecordProvider ds = getDataStore();
        for (InitUploadResult res : Lists.newArrayList(
                // 20MB upload and 10 URIs requested => should result in 2 URIs (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return TWENTY_MB; }
                    @Override public int getMaxNumURIs() { return 10; }
                    @Override public int getExpectedNumURIs() { return 2; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 100MB upload and 10 URIs requested => should result in 10 URIs (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_HUNDRED_MB; }
                    @Override public int getMaxNumURIs() { return 10; }
                    @Override public int getExpectedNumURIs() { return 10; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 100MB upload and 5 URIs requested => should result in 5 URIs (20MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_HUNDRED_MB; }
                    @Override public int getMaxNumURIs() { return 5; }
                    @Override public int getExpectedNumURIs() { return 5; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 50 URIs requested => should result in 50 URIs (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumURIs() { return 50; }
                    @Override public int getExpectedNumURIs() { return 50; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 10 URIs requested => should result in 10 URIs (50MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumURIs() { return 10; }
                    @Override public int getExpectedNumURIs() { return 10; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 60 URIs requested => should result in 50 uls (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumURIs() { return 60; }
                    @Override public int getExpectedNumURIs() { return 50; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 5 URIs requested => should result in 5 URIs (100MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumURIs() { return 5; }
                    @Override public int getExpectedNumURIs() { return 5; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 1GB upload and 10 URIs requested => should result in 10 URIs (100MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_GB; }
                    @Override public int getMaxNumURIs() { return 10; }
                    @Override public int getExpectedNumURIs() { return 10; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 5GB upload and 50 URIs requested => should result in 50 URIs (100MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_GB; }
                    @Override public int getMaxNumURIs() { return 50; }
                    @Override public int getExpectedNumURIs() { return 50; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                }
        )) {
            HttpDataRecordUpload uploadContext = ds.initiateHttpUpload(res.getUploadSize(), res.getMaxNumURIs());
            assertEquals(String.format("Failed for upload size: %d, num urls %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());
            assertEquals(String.format("Failed for upload size: %d, num urls %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedMinPartSize(), uploadContext.getMinPartSize());
            assertEquals(String.format("Failed for upload size: %d, num URIs %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedMaxPartSize(), uploadContext.getMaxPartSize());
        }
    }

    @Test
    public void testInitiateHttpUploadSizeTooBigForSinglePut() throws HttpUploadException {
        try {
            getDataStore().initiateHttpUpload(getProviderMaxSinglePutSize() + 1, 1);
            fail();
        }
        catch (UnsupportedHttpUploadArgumentsException e) { }
    }

    @Test
    public void testInitiateHttpUploadSizeTooBigForUpload() throws HttpUploadException {
        try {
            getDataStore().initiateHttpUpload(getProviderMaxBinaryUploadSize() + 1, -1);
            fail();
        }
        catch (UnsupportedHttpUploadArgumentsException e) { }
    }

    @Test
    public void testInititateHttpUploadRequestedPartSizesTooBig() throws HttpUploadException {
        try {
            getDataStore().initiateHttpUpload(FIVE_GB, 5);
            fail();
        }
        catch (UnsupportedHttpUploadArgumentsException e) { }
    }

    @Test
    public void testCompleteHttpUploadRequiresNonNullToken() throws UnsupportedHttpUploadArgumentsException, HttpUploadException, DataStoreException {
        try {
            getDataStore().completeHttpUpload(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteHttpUploadRequiresValidToken() throws UnsupportedHttpUploadArgumentsException, HttpUploadException, DataStoreException {
        for (String token : Lists.newArrayList("", "abc", "abc#123")) {
            try {
                getDataStore().completeHttpUpload(token);
                fail();
            }
            catch (IllegalArgumentException e) { }
        }
    }

    @Test
    public void testCompleteHttpUploadSignatureMustMatch() throws UnsupportedHttpUploadArgumentsException, HttpUploadException, DataStoreException {
        HttpDataRecordUpload uploadContext = getDataStore().initiateHttpUpload(ONE_MB, 1);

        // Pull the blob id out and modify it
        String uploadToken = uploadContext.getUploadToken();
        String[] parts = uploadToken.split("#");
        String tokenPart = parts[0];
        String sigPart = parts[1];
        String[] subParts = tokenPart.split("#");
        String blobId = subParts[0];
        char c = (char)((int)(blobId.charAt(blobId.length()-1))+1);
        blobId = blobId.substring(0, blobId.length()-1) + c;
        for (int i = 1; i<subParts.length; i++) {
            blobId += "#" + subParts[i];
        }
        String newToken = Base64.encode(blobId) + "#" + sigPart;

        try {
            getDataStore().completeHttpUpload(newToken);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testSinglePutDirectUploadIT() throws UnsupportedHttpUploadArgumentsException, HttpUploadException, DataStoreException, IOException {
        HttpDataRecordProvider ds = getDataStore();
        for (InitUploadResult res : Lists.newArrayList(
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_MB; }
                    @Override public int getMaxNumURIs() { return 10; }
                    @Override public int getExpectedNumURIs() { return 1; }
                    @Override public long getExpectedMinPartSize() { return TEN_MB; }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                }
        )) {
            DataRecord uploadedRecord = null;
            try {
                HttpDataRecordUpload uploadContext = ds.initiateHttpUpload(res.getUploadSize(), res.getMaxNumURIs());

                assertEquals(res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());
                String uploaded = randomString(res.getUploadSize());
                URI uploadURI = uploadContext.getUploadURIs().iterator().next();
                doHttpsUpload(new ByteArrayInputStream(uploaded.getBytes()), uploaded.length(), uploadURI);

                uploadedRecord = ds.completeHttpUpload(uploadContext.getUploadToken());
                assertNotNull(uploadedRecord);

                DataRecord retrievedRecord = doGetRecord((DataStore) ds, uploadedRecord.getIdentifier());
                assertNotNull(retrievedRecord);
                StringWriter writer = new StringWriter();
                IOUtils.copy(retrievedRecord.getStream(), writer, "utf-8");

                String retrieved = writer.toString();
                compareBinaries(uploaded, retrieved);
            }
            finally {
                if (null != uploadedRecord) {
                    doDeleteRecord((DataStore) ds, uploadedRecord.getIdentifier());
                }
            }
        }
    }

    //
    @Test
    public void testMultiPartDirectUploadIT() throws UnsupportedHttpUploadArgumentsException, HttpUploadException, DataStoreException, IOException {
        // Disabled by default - this test uses a lot of memory.
        // Execute this test from the command line like this:
        //   mvn test -Dtest=<child-class-name> -Dtest.opts.memory=-Xmx2G
        assumeTrue(integrationTestsEnabled());
        HttpDataRecordProvider ds = getDataStore();
        for (InitUploadResult res : Lists.newArrayList(
                new InitUploadResult() {
                    @Override public long getUploadSize() { return TWENTY_MB; }
                    @Override public int getMaxNumURIs() { return 10; }
                    @Override public int getExpectedNumURIs() { return 2; }
                    @Override public long getExpectedMinPartSize() { return TEN_MB; }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_HUNDRED_MB; }
                    @Override public int getMaxNumURIs() { return 10; }
                    @Override public int getExpectedNumURIs() { return 10; }
                    @Override public long getExpectedMinPartSize() { return TEN_MB; }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                }
        )) {
            DataRecord uploadedRecord = null;
            try {
                HttpDataRecordUpload uploadContext = ds.initiateHttpUpload(res.getUploadSize(), res.getMaxNumURIs());
                assertEquals(res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());

                String uploaded = randomString(res.getUploadSize());
                long uploadSize = res.getUploadSize();
                long uploadPartSize = uploadSize / uploadContext.getUploadURIs().size()
                        + ((uploadSize % uploadContext.getUploadURIs().size()) == 0 ? 0 : 1);
                ByteArrayInputStream in = new ByteArrayInputStream(uploaded.getBytes());

                assertTrue(uploadPartSize <= uploadContext.getMaxPartSize());
                assertTrue(uploadPartSize >= uploadContext.getMinPartSize());

                for (URI uri : uploadContext.getUploadURIs()) {
                    if (0 >= uploadSize) break;

                    long partSize = Math.min(uploadSize, uploadPartSize);
                    uploadSize -= partSize;

                    byte[] buffer = new byte[(int) partSize];
                    in.read(buffer, 0, (int) partSize);

                    doHttpsUpload(new ByteArrayInputStream(buffer), partSize, uri);
                }

                uploadedRecord = ds.completeHttpUpload(uploadContext.getUploadToken());
                assertNotNull(uploadedRecord);

                DataRecord retrievedRecord = doGetRecord((DataStore) ds, uploadedRecord.getIdentifier());
                assertNotNull(retrievedRecord);
                StringWriter writer = new StringWriter();
                IOUtils.copy(retrievedRecord.getStream(), writer, "utf-8");

                String retrieved = writer.toString();
                compareBinaries(uploaded, retrieved);
            }
            finally {
                if (null != uploadedRecord) {
                    doDeleteRecord((DataStore) ds, uploadedRecord.getIdentifier());
                }
            }
        }
    }

    private void compareBinaries(String uploaded, String retrieved) {
        assertEquals(uploaded.length(), retrieved.length());
        byte[] expectedSha = DigestUtils.sha256(uploaded.getBytes());
        byte[] actualSha = DigestUtils.sha256(retrieved.getBytes());
        assertEquals(expectedSha.length, actualSha.length);
        for (int i=0; i<expectedSha.length; i++) {
            assertEquals(expectedSha[i], actualSha[i]);
        }
    }

    protected Map<String, String> parseQueryString(URI uri) {
        Map<String, String> parsed = Maps.newHashMap();
        String query = uri.getQuery();
        try {
            for (String pair : query.split("&")) {
                String[] kv = pair.split("=", 2);
                parsed.put(URLDecoder.decode(kv[0], "utf-8"), URLDecoder.decode(kv[1], "utf-8"));
            }
        }
        catch (UnsupportedEncodingException e) {
            LOG.error("UnsupportedEncodingException caught", e);
        }
        return parsed;
    }

    protected String randomString(long size) {
        final String symbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringWriter writer = new StringWriter();
        Random random = new Random();
        if (size > 256) {
            String str256 = randomString(256);
            while (size >= 256) {
                writer.write(str256);
                size -= 256;
            }
            if (size > 0) {
                writer.write(str256.substring(0, (int)size));
            }
        }
        else {
            for (long i = 0; i < size; i++) {
                writer.append(symbols.charAt(random.nextInt(symbols.length())));
            }
        }
        return writer.toString();
    }

    protected void doHttpsUpload(InputStream in, long contentLength, URI uri) throws IOException {
        HttpsURLConnection conn = getHttpsConnection(contentLength, uri);
        IOUtils.copy(in, conn.getOutputStream());
        int responseCode = conn.getResponseCode();
        assertTrue(conn.getResponseMessage(), responseCode < 400);
    }

    interface InitUploadResult {
        long getUploadSize();
        int getMaxNumURIs();
        int getExpectedNumURIs();
        long getExpectedMinPartSize();
        long getExpectedMaxPartSize();
    }
}
