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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Lists;

import static org.apache.jackrabbit.guava.common.io.ByteStreams.toByteArray;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractDataRecordAccessProviderTest {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDataRecordAccessProviderTest.class);

    protected abstract ConfigurableDataRecordAccessProvider getDataStore();
    protected abstract ConfigurableDataRecordAccessProvider getDataStore(@NotNull Properties overrideProperties) throws Exception;
    protected abstract long getProviderMinPartSize();
    protected abstract long getProviderMaxPartSize();
    protected abstract long getProviderMaxSinglePutSize();
    protected abstract long getProviderMaxBinaryUploadSize();
    protected abstract boolean isSinglePutURI(URI url);
    protected abstract HttpsURLConnection getHttpsConnection(long length, URI url) throws IOException;
    protected abstract DataRecord doGetRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;
    protected abstract DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException;
    protected abstract void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;

    protected static int expirySeconds = 60*15;

    protected static long ONE_KB = 1024;
    protected static long ONE_MB = ONE_KB * ONE_KB;

    protected static long TEN_MB = ONE_MB * 10;
    protected static long TWENTY_MB = ONE_MB * 20;
    protected static long ONE_HUNDRED_MB = ONE_MB * 100;
    protected static long FIVE_HUNDRED_MB = ONE_HUNDRED_MB * 5;
    protected static long ONE_GB = ONE_HUNDRED_MB * 10;
    protected static long FIVE_GB = ONE_GB * 5;

    //
    // Direct download tests
    //
    @Test
    public void testGetDownloadURIProvidesValidURIIT() throws DataStoreException {
        DataRecord record = null;
        ConfigurableDataRecordAccessProvider dataStore = getDataStore();
        try {
            InputStream testStream = randomStream(0, 256);
            record = doSynchronousAddRecord((DataStore) dataStore, testStream);
            DataIdentifier id = record.getIdentifier();
            URI uri = getDataStore().getDownloadURI(id, DataRecordDownloadOptions.DEFAULT);
            assertNotNull(uri);
        }
        finally {
            if (null != record) {
                doDeleteRecord((DataStore) dataStore, record.getIdentifier());
            }
        }
    }

    @Test
    public void testGetDownloadURIWithExistsDisabled() throws Exception {
        DataIdentifier id = new DataIdentifier("identifier");
        Properties overrideProperties = new Properties();
        overrideProperties.put("presignedHttpDownloadURIVerifyExists", "false");
        ConfigurableDataRecordAccessProvider ds = getDataStore(overrideProperties);
        assertNotNull(ds.getDownloadURI(id, DataRecordDownloadOptions.DEFAULT));
    }

    @Test
    public void testGetDownloadURIRequiresValidIdentifierByDefault() {
        DataIdentifier id = new DataIdentifier("identifier");
        ConfigurableDataRecordAccessProvider ds = getDataStore();
        assertNull(ds.getDownloadURI(id, DataRecordDownloadOptions.DEFAULT));
    }

    @Test
    public void testGetDownloadURIRequiresNonNullIdentifier() {
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
        ConfigurableDataRecordAccessProvider dataStore = getDataStore();
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
        DataRecordAccessProvider dataStore = getDataStore();
        try {
            InputStream testStream = randomStream(0, 256);
            record = doSynchronousAddRecord((DataStore) dataStore, testStream);
            URI uri = dataStore.getDownloadURI(record.getIdentifier(), DataRecordDownloadOptions.DEFAULT);
            HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("GET");
            assertEquals(200, conn.getResponseCode());

            testStream.reset();
            assertTrue(Arrays.equals(toByteArray(testStream), toByteArray(conn.getInputStream())));
        }
        finally {
            if (null != record) {
                doDeleteRecord((DataStore) dataStore, record.getIdentifier());
            }
        }
    }

    @Test
    public void testGetDownloadURIWithCustomHeadersIT() throws DataStoreException, IOException {
        String umlautFilename = "Uml\u00e4utfile.png";
        String umlautFilename_ISO_8859_1 = new String(
                StandardCharsets.ISO_8859_1.encode(umlautFilename).array(),
                StandardCharsets.ISO_8859_1
        );
        List<String> fileNames = List.of(
                "image.png",
                "beautiful landscape.png",
                "\"filename-with-double-quotes\".png",
                "filename-with-one\"double-quote.jpg",
                umlautFilename
                );
        List<String> iso_8859_1_fileNames = List.of(
                "image.png",
                "beautiful landscape.png",
                "\\\"filename-with-double-quotes\\\".png",
                "filename-with-one\\\"double-quote.jpg",
                umlautFilename_ISO_8859_1
        );
        List<String> rfc8187_fileNames = List.of(
                "image.png",
                "beautiful%20landscape.png",
                "%22filename-with-double-quotes%22.png",
                "filename-with-one%22double-quote.jpg",
                "Uml%C3%A4utfile.png"
        );

        DataRecord record = null;
        DataRecordAccessProvider dataStore = getDataStore();
        try {
            InputStream testStream = randomStream(0, 256);
            record = doSynchronousAddRecord((DataStore) dataStore, testStream);
            String mimeType = "image/png";
            String dispositionType = "inline";
            for (int i=0; i<fileNames.size(); i++) {
                String fileName = fileNames.get(i);
                String iso_8859_1_fileName = iso_8859_1_fileNames.get(i);
                String encodedFileName = rfc8187_fileNames.get(i);
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
                        downloadOptions
                );

                HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
                conn.setRequestMethod("GET");
                assertEquals(200, conn.getResponseCode());

                assertEquals(mimeType, conn.getHeaderField("Content-Type"));
                assertEquals(
                        String.format("%s; filename=\"%s\"; filename*=UTF-8''%s",
                                dispositionType, iso_8859_1_fileName, encodedFileName
                        ),
                        conn.getHeaderField("Content-Disposition")
                );

                testStream.reset();
                assertTrue(Arrays.equals(toByteArray(testStream), toByteArray(conn.getInputStream())));
            }
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
        ConfigurableDataRecordAccessProvider dataStore = getDataStore();
        try {
            dataStore.setDirectDownloadURIExpirySeconds(2);
            record = doSynchronousAddRecord((DataStore) dataStore, randomStream(0, 256));
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

    @Test
    public void testGetDownloadURINonexistentBlobFailsIT() throws DataStoreException {
        ConfigurableDataRecordAccessProvider dataStore = getDataStore();
        InputStream testStream = randomStream(0, 256);

        DataRecord record = doSynchronousAddRecord((DataStore) dataStore, testStream);

        doDeleteRecord((DataStore) dataStore, record.getIdentifier());

        URI uri = dataStore.getDownloadURI(record.getIdentifier(), DataRecordDownloadOptions.DEFAULT);

        assertNull(uri);
    }

    //
    // Direct upload tests
    //
    @Test
    public void testInitiateDirectUploadReturnsValidUploadContext() throws DataRecordUploadException {
        DataRecordUpload uploadContext =
                getDataStore().initiateDataRecordUpload(ONE_MB, 10);
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
    public void testInitiateDirectUploadRequiresNonzeroFileSize() throws DataRecordUploadException {
        try {
            getDataStore().initiateDataRecordUpload(0, 10);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInititateDirectUploadRequiresNonzeroNumURIs() throws DataRecordUploadException {
        try {
            getDataStore().initiateDataRecordUpload(ONE_MB, 0);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateDirectUploadRequiresNonNegativeNumURIs()
            throws DataRecordUploadException {
        try {
            getDataStore().initiateDataRecordUpload(ONE_MB, -2);
            fail();
        }
        catch (IllegalArgumentException e) { }

        // -1 is allowed which means any number of URIs
        try {
            assertNotNull(getDataStore().initiateDataRecordUpload(ONE_HUNDRED_MB, -1));
        }
        catch (IllegalArgumentException e) {
            fail();
        }
    }

    @Test
    public void testInititateDirectUploadSingleURIRequested() throws DataRecordUploadException {
        DataRecordUpload uploadContext =
                getDataStore().initiateDataRecordUpload(TWENTY_MB, 1);
        assertEquals(1, uploadContext.getUploadURIs().size());
        assertTrue(isSinglePutURI(uploadContext.getUploadURIs().iterator().next()));
    }

    @Test
    public void testInititateDirectUploadSizeLowerThanMinPartSize() throws DataRecordUploadException {
        DataRecordUpload uploadContext =
                getDataStore().initiateDataRecordUpload(getProviderMinPartSize()-1L, 10);
        assertEquals(1, uploadContext.getUploadURIs().size());
        assertTrue(isSinglePutURI(uploadContext.getUploadURIs().iterator().next()));
    }

    @Test
    public void testInititateDirectUploadMultiPartDisabled() throws DataRecordUploadException {
        ConfigurableDataRecordAccessProvider ds = getDataStore();
        try {
            ds.setDirectUploadURIExpirySeconds(0);
            assertNull(ds.initiateDataRecordUpload(TWENTY_MB, 10));
            assertNull(ds.initiateDataRecordUpload(20, 1));
        }
        finally {
            ds.setDirectUploadURIExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInititateDirectUploadURIListSizes() throws DataRecordUploadException {
        DataRecordAccessProvider ds = getDataStore();
        for (InitUploadResult res : List.of(
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
            DataRecordUpload uploadContext = ds.initiateDataRecordUpload(res.getUploadSize(), res.getMaxNumURIs());
            assertEquals(String.format("Failed for upload size: %d, num URIs %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());
            assertEquals(String.format("Failed for upload size: %d, num URIs %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedMinPartSize(), uploadContext.getMinPartSize());
            assertEquals(String.format("Failed for upload size: %d, num URIs %d", res.getUploadSize(), res.getMaxNumURIs()),
                    res.getExpectedMaxPartSize(), uploadContext.getMaxPartSize());
        }
    }

    @Test
    public void testInitiateDirectUploadSizeTooBigForSinglePut() throws DataRecordUploadException {
        try {
            getDataStore().initiateDataRecordUpload(getProviderMaxSinglePutSize() + 1, 1);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateDirectUploadSizeTooBigForUpload() throws DataRecordUploadException {
        try {
            getDataStore().initiateDataRecordUpload(getProviderMaxBinaryUploadSize() + 1, -1);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInititateDirectUploadRequestedPartSizesTooBig() throws DataRecordUploadException {
        try {
            getDataStore().initiateDataRecordUpload(FIVE_GB, 5);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteDirectUploadRequiresNonNullToken() throws DataRecordUploadException, DataStoreException {
        try {
            getDataStore().completeDataRecordUpload(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteDirectUploadRequiresValidToken() throws DataRecordUploadException, DataStoreException {
        for (String token : List.of("", "abc", "abc#123")) {
            try {
                getDataStore().completeDataRecordUpload(token);
                fail();
            }
            catch (IllegalArgumentException e) { }
        }
    }

    @Test
    public void testCompleteDirectUploadSignatureMustMatch() throws DataRecordUploadException, DataStoreException {
        DataRecordUpload uploadContext = getDataStore().initiateDataRecordUpload(ONE_MB, 1);

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
            getDataStore().completeDataRecordUpload(newToken);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteAlreadyUploadedBinaryReturnsSameBinaryIT() throws DataStoreException, DataRecordUploadException, IOException {
        DataRecordAccessProvider ds = getDataStore();
        DataRecord uploadedRecord = null;
        try {
            DataRecordUpload uploadContext = ds.initiateDataRecordUpload(ONE_MB, 1);
            InputStream uploadStream = randomStream(0, ONE_MB);
            URI uploadURI = uploadContext.getUploadURIs().iterator().next();
            doHttpsUpload(uploadStream, ONE_MB, uploadURI);
            uploadedRecord = ds.completeDataRecordUpload(uploadContext.getUploadToken());
            assertEquals(ONE_MB, uploadedRecord.getLength());

            DataRecord secondRecord = ds.completeDataRecordUpload(uploadContext.getUploadToken());

            assertEquals(uploadedRecord.getIdentifier(), secondRecord.getIdentifier());
            assertEquals(uploadedRecord.getLength(), secondRecord.getLength());
            StringWriter original = new StringWriter();
            IOUtils.copy(uploadedRecord.getStream(), original, Charset.forName("UTF-8"));
            StringWriter second = new StringWriter();
            IOUtils.copy(secondRecord.getStream(), second, Charset.forName("UTF-8"));
            assertEquals(original.toString(), second.toString());
        }
        finally {
            if (null != uploadedRecord) {
                doDeleteRecord((DataStore) ds, uploadedRecord.getIdentifier());
            }
        }
    }

    @Test
    public void testSinglePutDirectUploadIT() throws DataRecordUploadException, DataStoreException, IOException {
        DataRecordAccessProvider ds = getDataStore();
        for (InitUploadResult res : List.of(
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
                DataRecordUpload uploadContext = ds.initiateDataRecordUpload(res.getUploadSize(), res.getMaxNumURIs());

                assertEquals(res.getExpectedNumURIs(), uploadContext.getUploadURIs().size());

                InputStream uploadStream = randomStream(0, (int) res.getUploadSize());
                URI uploadURI = uploadContext.getUploadURIs().iterator().next();
                doHttpsUpload(uploadStream, res.getUploadSize(), uploadURI);

                uploadedRecord = ds.completeDataRecordUpload(uploadContext.getUploadToken());
                assertNotNull(uploadedRecord);

                DataRecord retrievedRecord = doGetRecord((DataStore) ds, uploadedRecord.getIdentifier());
                assertNotNull(retrievedRecord);
                uploadStream.reset();
                assertTrue(Arrays.equals(toByteArray(uploadStream), toByteArray(retrievedRecord.getStream())));
            }
            finally {
                if (null != uploadedRecord) {
                    doDeleteRecord((DataStore) ds, uploadedRecord.getIdentifier());
                }
            }
        }
    }

    protected Map<String, String> parseQueryString(URI uri) {
        Map<String, String> parsed = new HashMap<>();
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
