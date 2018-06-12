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

package org.apache.jackrabbit.oak.spi.blob;

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

import javax.net.ssl.HttpsURLConnection;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class AbstractURLWritableBlobStoreTest {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractURLReadableBlobStoreTest.class);

    protected static int expirySeconds = 60*15;
    protected static long ONE_KB = 1024;
    protected static long ONE_MB = ONE_KB * ONE_KB;

    protected static long TEN_MB = ONE_MB * 10;
    protected static long TWENTY_MB = ONE_MB * 20;
    protected static long ONE_HUNDRED_MB = ONE_MB * 100;
    protected static long FIVE_HUNDRED_MB = ONE_HUNDRED_MB * 5;
    protected static long ONE_GB = ONE_HUNDRED_MB * 10;
    protected static long FIVE_GB = ONE_GB * 5;

    protected abstract URLWritableDataStore getDataStore();
    protected abstract void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException;
    protected abstract long getProviderMinPartSize();
    protected abstract long getProviderMaxPartSize();
    protected abstract boolean isSinglePutURL(URL url);
    protected abstract HttpsURLConnection getHttpsConnection(long length, URL url) throws IOException;

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
    public void testInitDirectUploadReturnsValidContext() throws DirectBinaryAccessException {
        URLWritableDataStoreUploadContext context =
                getDataStore().initDirectUpload(ONE_MB, 10);
        assertNotNull(context);
        assertFalse(context.getUploadPartURLs().isEmpty());
        assertTrue(context.getMinPartSize() > 0);
        assertTrue(context.getMinPartSize() >= getProviderMinPartSize());
        assertTrue(context.getMaxPartSize() >= context.getMinPartSize());
        assertTrue(context.getMaxPartSize() <= getProviderMaxPartSize());
        assertTrue((context.getMaxPartSize() * context.getUploadPartURLs().size()) >= ONE_MB);
        assertFalse(Strings.isNullOrEmpty(context.getUploadToken()));
    }

    @Test
    public void testInitDirectUploadRequiresNonzeroFileSize() {
        try {
            getDataStore().initDirectUpload(0, 10);
            fail();
        }
        catch (DirectBinaryAccessException e) { }
    }

    @Test
    public void testInitDirectUploadRequiresNonzeroNumURLs() {
        try {
            getDataStore().initDirectUpload(ONE_MB, 0);
            fail();
        }
        catch (DirectBinaryAccessException e) { }
    }

    @Test
    public void testInitDirectUploadSingleURLRequested() throws DirectBinaryAccessException {
        URLWritableDataStoreUploadContext context =
                getDataStore().initDirectUpload(TWENTY_MB, 1);
        assertEquals(1, context.getUploadPartURLs().size());
        assertTrue(isSinglePutURL(context.getUploadPartURLs().get(0)));
    }

    @Test
    public void testInitDirectUploadSizeLowerThanMinPartSize() throws DirectBinaryAccessException {
        URLWritableDataStoreUploadContext context =
                getDataStore().initDirectUpload(getProviderMinPartSize()-1L, 10);
        assertEquals(1, context.getUploadPartURLs().size());
        assertTrue(isSinglePutURL(context.getUploadPartURLs().get(0)));
    }

    @Test
    public void testInitDirectUploadMultiPartDisabled() throws DirectBinaryAccessException {
        URLWritableDataStore ds = getDataStore();
        try {
            ds.setURLWritableBinaryExpirySeconds(0);
            URLWritableDataStoreUploadContext context = ds.initDirectUpload(TWENTY_MB, 10);
            assertEquals(0, context.getUploadPartURLs().size());
        }
        finally {
            ds.setURLWritableBinaryExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInitDirectUploadReturnsValidUploadToken() throws DirectBinaryAccessException {
        URLWritableDataStoreUploadContext context = getDataStore().initDirectUpload(ONE_MB, 1);
        String token = context.getUploadToken();
        assertTrue(isValidUploadToken(token));
    }

    @Test
    public void testInitDirectUploadURLListSizes() throws DirectBinaryAccessException {
        URLWritableDataStore ds = getDataStore();
        for (InitUploadResult res : Lists.newArrayList(
                // 20MB upload and 10 URLs requested => should result in 2 urls (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return TWENTY_MB; }
                    @Override public int getMaxNumUrls() { return 10; }
                    @Override public int getExpectedNumUrls() { return 2; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 100MB upload and 10 URLs requested => should result in 10 urls (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_HUNDRED_MB; }
                    @Override public int getMaxNumUrls() { return 10; }
                    @Override public int getExpectedNumUrls() { return 10; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 100MB upload and 5 URLs requested => should result in 5 urls (20MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_HUNDRED_MB; }
                    @Override public int getMaxNumUrls() { return 5; }
                    @Override public int getExpectedNumUrls() { return 5; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 50 URLs requested => should result in 50 urls (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumUrls() { return 50; }
                    @Override public int getExpectedNumUrls() { return 50; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 10 URLs requested => should result in 10 urls (50MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumUrls() { return 10; }
                    @Override public int getExpectedNumUrls() { return 10; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 60 URLs requested => should result in 50 uls (10MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumUrls() { return 60; }
                    @Override public int getExpectedNumUrls() { return 50; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 500MB upload and 5 URLs requested => should result in 5 urls (100MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_HUNDRED_MB; }
                    @Override public int getMaxNumUrls() { return 5; }
                    @Override public int getExpectedNumUrls() { return 5; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 1GB upload and 10 URLs requested => should result in 10 urls (100MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_GB; }
                    @Override public int getMaxNumUrls() { return 10; }
                    @Override public int getExpectedNumUrls() { return 10; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                // 5GB upload and 50 URLs requested => should result in 50 urls (100MB each)
                new InitUploadResult() {
                    @Override public long getUploadSize() { return FIVE_GB; }
                    @Override public int getMaxNumUrls() { return 50; }
                    @Override public int getExpectedNumUrls() { return 50; }
                    @Override public long getExpectedMinPartSize() { return getProviderMinPartSize(); }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                }
        )) {
            URLWritableDataStoreUploadContext context = ds.initDirectUpload(res.getUploadSize(), res.getMaxNumUrls());
            assertEquals(String.format("Failed for upload size: %d, num urls %d", res.getUploadSize(), res.getMaxNumUrls()),
                    res.getExpectedNumUrls(), context.getUploadPartURLs().size());
            assertEquals(String.format("Failed for upload size: %d, num urls %d", res.getUploadSize(), res.getMaxNumUrls()),
                    res.getExpectedMinPartSize(), context.getMinPartSize());
            assertEquals(String.format("Failed for upload size: %d, num urls %d", res.getUploadSize(), res.getMaxNumUrls()),
                    res.getExpectedMaxPartSize(), context.getMaxPartSize());
        }
    }

    @Test
    public void InitDirectUploadRequestedPartSizesTooBig() {
        try {
            getDataStore().initDirectUpload(FIVE_GB, 5);
            fail();
        }
        catch (DirectBinaryAccessException e) { }
    }

    @Test
    public void testCompleteDirectUploadRequiresNonNullToken() throws DirectBinaryAccessException, DataStoreException {
        try {
            getDataStore().completeDirectUpload(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteDirectUploadRequiresValidToken() throws DirectBinaryAccessException, DataStoreException {
        for (String token : Lists.newArrayList("", "abc", "abc#123")) {
            try {
                getDataStore().completeDirectUpload(token);
                fail();
            }
            catch (IllegalArgumentException e) { }
        }
    }

    @Test
    public void testCompleteDirectUploadSignatureMustMatch() throws DirectBinaryAccessException, DataStoreException {
        URLWritableDataStoreUploadContext context = getDataStore().initDirectUpload(ONE_MB, 1);

        // Pull the blob id out and modify it
        String uploadToken = context.getUploadToken();
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
    public void testSinglePutDirectUploadIT() throws DirectBinaryAccessException, DataStoreException, IOException {
        URLWritableDataStore ds = getDataStore();
        for (InitUploadResult res : Lists.newArrayList(
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_MB; }
                    @Override public int getMaxNumUrls() { return 10; }
                    @Override public int getExpectedNumUrls() { return 1; }
                    @Override public long getExpectedMinPartSize() { return TEN_MB; }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                }
        )) {
            DataRecord uploadedRecord = null;
            try {
                URLWritableDataStoreUploadContext context = ds.initDirectUpload(res.getUploadSize(), res.getMaxNumUrls());

                assertEquals(res.getExpectedNumUrls(), context.getUploadPartURLs().size());
                String uploaded = randomString(res.getUploadSize());
                URL uploadUrl = context.getUploadPartURLs().get(0);
                doHttpsUpload(new ByteArrayInputStream(uploaded.getBytes()), uploaded.length(), uploadUrl);

                uploadedRecord = ds.completeDirectUpload(context.getUploadToken());
                assertNotNull(uploadedRecord);

                DataRecord retrievedRecord = ds.getRecord(uploadedRecord.getIdentifier());
                assertNotNull(retrievedRecord);
                StringWriter writer = new StringWriter();
                IOUtils.copy(retrievedRecord.getStream(), writer, "utf-8");

                String retrieved = writer.toString();
                compareBinaries(uploaded, retrieved);
            }
            finally {
                if (null != uploadedRecord) {
                    doDeleteRecord(ds, uploadedRecord.getIdentifier());
                }
            }
        }
    }

    @Test
    public void testMultiPartDirectUploadIT() throws DirectBinaryAccessException, DataStoreException, IOException {
        URLWritableDataStore ds = getDataStore();
        for (InitUploadResult res : Lists.newArrayList(
                new InitUploadResult() {
                    @Override public long getUploadSize() { return TWENTY_MB; }
                    @Override public int getMaxNumUrls() { return 10; }
                    @Override public int getExpectedNumUrls() { return 2; }
                    @Override public long getExpectedMinPartSize() { return TEN_MB; }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                },
                new InitUploadResult() {
                    @Override public long getUploadSize() { return ONE_HUNDRED_MB; }
                    @Override public int getMaxNumUrls() { return 10; }
                    @Override public int getExpectedNumUrls() { return 10; }
                    @Override public long getExpectedMinPartSize() { return TEN_MB; }
                    @Override public long getExpectedMaxPartSize() { return getProviderMaxPartSize(); }
                }
        )) {
            DataRecord uploadedRecord = null;
            try {
                URLWritableDataStoreUploadContext context = ds.initDirectUpload(res.getUploadSize(), res.getMaxNumUrls());
                assertEquals(res.getExpectedNumUrls(), context.getUploadPartURLs().size());

                String uploaded = randomString(res.getUploadSize());
                long uploadSize = res.getUploadSize();
                long uploadPartSize = uploadSize / context.getUploadPartURLs().size()
                        + ((uploadSize % context.getUploadPartURLs().size()) == 0 ? 0 : 1);
                ByteArrayInputStream in = new ByteArrayInputStream(uploaded.getBytes());

                assertTrue(uploadPartSize <= context.getMaxPartSize());
                assertTrue(uploadPartSize >= context.getMinPartSize());

                for (URL url : context.getUploadPartURLs()) {
                    if (0 >= uploadSize) break;

                    long partSize = Math.min(uploadSize, uploadPartSize);
                    uploadSize -= partSize;

                    byte[] buffer = new byte[(int) partSize];
                    in.read(buffer, 0, (int) partSize);

                    doHttpsUpload(new ByteArrayInputStream(buffer), partSize, url);
                }

                uploadedRecord = ds.completeDirectUpload(context.getUploadToken());
                assertNotNull(uploadedRecord);

                DataRecord retrievedRecord = ds.getRecord(uploadedRecord.getIdentifier());
                assertNotNull(retrievedRecord);
                StringWriter writer = new StringWriter();
                IOUtils.copy(retrievedRecord.getStream(), writer, "utf-8");

                String retrieved = writer.toString();
                compareBinaries(uploaded, retrieved);
            }
            finally {
                if (null != uploadedRecord) {
                    doDeleteRecord(ds, uploadedRecord.getIdentifier());
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

    protected Map<String, String> parseQueryString(URL url) {
        Map<String, String> parsed = Maps.newHashMap();
        String query = url.getQuery();
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

    protected boolean isValidUploadToken(String uploadToken) {
        return null != DirectBinaryUploadToken.fromEncodedToken(uploadToken);
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

    protected void doHttpsUpload(InputStream in, long contentLength, URL url) throws IOException {
        HttpsURLConnection conn = getHttpsConnection(contentLength, url);
        IOUtils.copy(in, conn.getOutputStream());
        int responseCode = conn.getResponseCode();
        assertTrue(conn.getResponseMessage(), responseCode < 400);
    }

    interface InitUploadResult {
        long getUploadSize();
        int getMaxNumUrls();
        int getExpectedNumUrls();
        long getExpectedMinPartSize();
        long getExpectedMaxPartSize();
    }
}
