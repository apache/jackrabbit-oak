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
package org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess;

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
import java.nio.charset.StandardCharsets;
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
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.util.Base64;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataRecordDirectAccessProviderTest {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDataRecordDirectAccessProviderTest.class);

    protected abstract ConfigurableDataRecordDirectAccessProvider getDataStore();
    protected abstract long getProviderMinPartSize();
    protected abstract long getProviderMaxPartSize();
    protected abstract long getProviderMaxSinglePutSize();
    protected abstract long getProviderMaxBinaryUploadSize();
    protected abstract boolean isSinglePutURI(URI url);
    protected abstract HttpsURLConnection getHttpsConnection(long length, URI url) throws IOException;
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
    // Direct download tests
    //
    @Test
    public void testGetDownloadURIProvidesValidURI() {
        DataIdentifier id = new DataIdentifier("testIdentifier");
        URI uri = getDataStore().getDownloadURI(id, DataRecordDownloadOptions.DEFAULT);
        assertNotNull(uri);
    }

    @Test
    public void testGetDownloadURIRequiresValidIdentifier() {
        try {
            getDataStore().getDownloadURI(null, DataRecordDownloadOptions.DEFAULT);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testGetDownloadURIRequiresValidDownloadOptions() {
        try {
            getDataStore().getDownloadURI(new DataIdentifier("testIdentifier"),
                    null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testGetDownloadURIExpirationOfZeroFails() {
        ConfigurableDataRecordDirectAccessProvider dataStore = getDataStore();
        try {
            dataStore.setDirectDownloadURIExpirySeconds(0);
            assertNull(dataStore.getDownloadURI(new DataIdentifier("testIdentifier"), DataRecordDownloadOptions.DEFAULT));
        }
        finally {
            dataStore.setDirectDownloadURIExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testGetDownloadURIIT() throws DataStoreException, IOException {
        DataRecord record = null;
        DataRecordDirectAccessProvider dataStore = getDataStore();
        try {
            String testData = randomString(256);
            record = doSynchronousAddRecord((DataStore) dataStore,
                    new ByteArrayInputStream(testData.getBytes()));
            URI uri = dataStore.getDownloadURI(record.getIdentifier(), DataRecordDownloadOptions.DEFAULT);
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
    public void testGetDownloadURIWithCustomHeadersIT() throws DataStoreException, IOException {
        DataRecord record = null;
        DataRecordDirectAccessProvider dataStore = getDataStore();
        try {
            String testData = randomString(256);
            record = doSynchronousAddRecord((DataStore) dataStore,
                    new ByteArrayInputStream(testData.getBytes()));
            String mimeType = "image/png";
            String fileName = "album cover.png";
            String dispositionType = "inline";
            DataRecordDownloadOptions downloadOptions =
                    DataRecordDownloadOptions.fromBlobDownloadOptions(
                            new BlobDownloadOptions(
                                    mimeType,
                                    null,
                                    fileName,
                                    dispositionType
                            )
                    );
            URI uri = dataStore.getDownloadURI(record.getIdentifier(),
                    downloadOptions);

            HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("GET");
            assertEquals(200, conn.getResponseCode());

            assertEquals(mimeType, conn.getHeaderField("Content-Type"));
            assertEquals(
                    String.format("%s; filename=\"%s\"; filename*=UTF-8''%s",
                            dispositionType, fileName,
                            new String(fileName.getBytes(StandardCharsets.UTF_8))
                    ),
                    conn.getHeaderField("Content-Disposition")
            );

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
        ConfigurableDataRecordDirectAccessProvider dataStore = getDataStore();
        try {
            String testData = randomString(256);
            dataStore.setDirectDownloadURIExpirySeconds(2);
            record = doSynchronousAddRecord((DataStore) dataStore,
                    new ByteArrayInputStream(testData.getBytes()));
            URI uri = dataStore.getDownloadURI(record.getIdentifier(), DataRecordDownloadOptions.DEFAULT);
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
            dataStore.setDirectDownloadURIExpirySeconds(expirySeconds);
        }
    }

    //
    // Direct upload tests
    //
    @Test
    public void testInitiateDirectUploadReturnsValidUploadContext() throws DataRecordDirectUploadException {
        DataRecordUpload uploadContext =
                getDataStore().initiateDirectUpload(ONE_MB, 10);
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
    public void testInitiateDirectUploadRequiresNonzeroFileSize() throws DataRecordDirectUploadException {
        try {
            getDataStore().initiateDirectUpload(0, 10);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInititateDirectUploadRequiresNonzeroNumURIs() throws DataRecordDirectUploadException {
        try {
            getDataStore().initiateDirectUpload(ONE_MB, 0);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateDirectUploadRequiresNonNegativeNumURIs()
            throws DataRecordDirectUploadException {
        try {
            getDataStore().initiateDirectUpload(ONE_MB, -2);
            fail();
        }
        catch (IllegalArgumentException e) { }

        // -1 is allowed which means any number of URIs
        try {
            assertNotNull(getDataStore().initiateDirectUpload(ONE_HUNDRED_MB, -1));
        }
        catch (IllegalArgumentException e) {
            fail();
        }
    }

    @Test
    public void testInititateDirectUploadSingleURIRequested() throws DataRecordDirectUploadException {
        DataRecordUpload uploadContext =
                getDataStore().initiateDirectUpload(TWENTY_MB, 1);
        assertEquals(1, uploadContext.getUploadURIs().size());
        assertTrue(isSinglePutURI(uploadContext.getUploadURIs().iterator().next()));
    }

    @Test
    public void testInititateDirectUploadSizeLowerThanMinPartSize() throws DataRecordDirectUploadException {
        DataRecordUpload uploadContext =
                getDataStore().initiateDirectUpload(getProviderMinPartSize()-1L, 10);
        assertEquals(1, uploadContext.getUploadURIs().size());
        assertTrue(isSinglePutURI(uploadContext.getUploadURIs().iterator().next()));
    }

    @Test
    public void testInititateDirectUploadMultiPartDisabled() throws DataRecordDirectUploadException {
        ConfigurableDataRecordDirectAccessProvider ds = getDataStore();
        try {
            ds.setDirectUploadURIExpirySeconds(0);
            DataRecordUpload uploadContext = ds.initiateDirectUpload(TWENTY_MB, 10);
            assertEquals(0, uploadContext.getUploadURIs().size());

            uploadContext = ds.initiateDirectUpload(20, 1);
            assertEquals(0, uploadContext.getUploadURIs().size());
        }
        finally {
            ds.setDirectUploadURIExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInititateDirectUploadURIListSizes() throws DataRecordDirectUploadException {
        DataRecordDirectAccessProvider ds = getDataStore();
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
            DataRecordUpload uploadContext = ds.initiateDirectUpload(res.getUploadSize(), res.getMaxNumURIs());
            assertEquals(String.format("Failed for upload size: %d, num URIs %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());
            assertEquals(String.format("Failed for upload size: %d, num URIs %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedMinPartSize(), uploadContext.getMinPartSize());
            assertEquals(String.format("Failed for upload size: %d, num URIs %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedMaxPartSize(), uploadContext.getMaxPartSize());
        }
    }

    @Test
    public void testInitiateDirectUploadSizeTooBigForSinglePut() throws DataRecordDirectUploadException {
        try {
            getDataStore().initiateDirectUpload(getProviderMaxSinglePutSize() + 1, 1);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateDirectUploadSizeTooBigForUpload() throws DataRecordDirectUploadException {
        try {
            getDataStore().initiateDirectUpload(getProviderMaxBinaryUploadSize() + 1, -1);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInititateDirectUploadRequestedPartSizesTooBig() throws DataRecordDirectUploadException {
        try {
            getDataStore().initiateDirectUpload(FIVE_GB, 5);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteDirectUploadRequiresNonNullToken() throws DataRecordDirectUploadException, DataStoreException {
        try {
            getDataStore().completeDirectUpload(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteDirectUploadRequiresValidToken() throws DataRecordDirectUploadException, DataStoreException {
        for (String token : Lists.newArrayList("", "abc", "abc#123")) {
            try {
                getDataStore().completeDirectUpload(token);
                fail();
            }
            catch (IllegalArgumentException e) { }
        }
    }

    @Test
    public void testCompleteDirectUploadSignatureMustMatch() throws DataRecordDirectUploadException, DataStoreException {
        DataRecordUpload uploadContext = getDataStore().initiateDirectUpload(ONE_MB, 1);

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
            getDataStore().completeDirectUpload(newToken);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testSinglePutDirectUploadIT() throws DataRecordDirectUploadException, DataStoreException, IOException {
        DataRecordDirectAccessProvider ds = getDataStore();
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
                DataRecordUpload uploadContext = ds.initiateDirectUpload(res.getUploadSize(), res.getMaxNumURIs());

                assertEquals(res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());
                String uploaded = randomString(res.getUploadSize());
                URI uploadURI = uploadContext.getUploadURIs().iterator().next();
                doHttpsUpload(new ByteArrayInputStream(uploaded.getBytes()), uploaded.length(), uploadURI);

                uploadedRecord = ds.completeDirectUpload(uploadContext.getUploadToken());
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
    public void testMultiPartDirectUploadIT() throws DataRecordDirectUploadException, DataStoreException, IOException {
        // Disabled by default - this test uses a lot of memory.
        // Execute this test from the command line like this:
        //   mvn test -Dtest=<child-class-name> -Dtest.opts.memory=-Xmx2G
        assumeTrue(integrationTestsEnabled());
        DataRecordDirectAccessProvider ds = getDataStore();
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
                DataRecordUpload uploadContext = ds.initiateDirectUpload(res.getUploadSize(), res.getMaxNumURIs());
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

                uploadedRecord = ds.completeDirectUpload(uploadContext.getUploadToken());
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
