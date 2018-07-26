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
package org.apache.jackrabbit.oak.jcr.binary;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.SECONDS;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.createFileWithBinary;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.getBinary;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.getOrCreateNtFile;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.getRandomString;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.getTestInputStream;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.httpGet;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.httpPut;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.httpPutTestStream;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.isFailedHttpPut;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.isSuccessfulHttpPut;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.putBinary;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.saveFileWithBinary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordDirectAccessProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration test for direct binary GET/PUT via HTTP, that requires a fully working data store
 * (such as S3) for each {@link AbstractHttpBinaryIT#dataStoreFixtures() configured fixture}.
 * The data store in question must support direct GET/PUT access via a URI.
 * 
 * Data store must be configured through e.g. aws.properties.
 *
 * Run this IT in maven using either:
 *
 *   single test:
 *     mvn clean test -Dtest=HttpBinaryIT
 * 
 *   as part of all integration tests:
 *     mvn -PintegrationTesting clean install
 */
@RunWith(Parameterized.class)
public class HttpBinaryIT extends AbstractHttpBinaryIT {

    private static final String FILE_PATH = "/file";
    private static final int REGULAR_WRITE_EXPIRY = 60*5; // seconds
    private static final int REGULAR_READ_EXPIRY = 60*5; // seconds

    public HttpBinaryIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    private Session adminSession;
    private Session anonymousSession;
    private JackrabbitValueFactory uploadProvider;
    private JackrabbitValueFactory anonymousUploadProvider;

    @Before
    public void cleanRepoContents() throws RepositoryException {
        adminSession = getAdminSession();
        anonymousSession = getAnonymousSession();
        uploadProvider = (JackrabbitValueFactory) adminSession.getValueFactory();
        anonymousUploadProvider = (JackrabbitValueFactory) anonymousSession.getValueFactory();

        if (adminSession.nodeExists(FILE_PATH)) {
            adminSession.getNode(FILE_PATH).remove();
            adminSession.save();
        }
    }

    // F1 - basic test
    @Test
    public void testUpload() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        final String content = getRandomString(256);
        final long size = content.getBytes(StandardCharsets.UTF_8).length;

        assertTrue(adminSession.getValueFactory() instanceof JackrabbitValueFactory);

        BinaryUpload upload = uploadProvider.initiateBinaryUpload(size, 1);
        assertNotNull(upload);

        // very small test binary
        assertTrue(size < upload.getMaxPartSize());

        URI uri = upload.getUploadURIs().iterator().next();
        assertNotNull(uri);

        LOG.info("- uploading binary via PUT to {}", uri.toString());
        int code = httpPut(uri, content.getBytes().length, getTestInputStream(content));

        assertTrue("PUT to pre-signed S3 URI failed",
                isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary writeBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        putBinary(adminSession, FILE_PATH, writeBinary);

        Binary readBinary = getBinary(adminSession, FILE_PATH);
        StringWriter writer = new StringWriter();
        IOUtils.copy(readBinary.getStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // F3 - Multi-part upload
    @Test
    public void testMultiPartUpload() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        assertTrue(adminSession.getValueFactory() instanceof JackrabbitValueFactory);

        // 25MB is a good size to ensure chunking is done
        long uploadSize = 1024 * 1024 * 25;
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(uploadSize, 50);
        assertNotNull(upload);

        String buffer = getRandomString(uploadSize);
        long uploadPartSize = upload.getMaxPartSize();
        long remaining = uploadSize;
        for (URI uri : upload.getUploadURIs()) {
            String nextPart = buffer.substring(0, (int) Math.min(uploadPartSize, remaining));
            int code = httpPut(uri,
                    nextPart.getBytes().length,
                    new ByteArrayInputStream(nextPart.getBytes()),
                    true);
            assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
            remaining -= uploadPartSize;
            if (remaining <= 0) break;
        }

        Binary writeBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        putBinary(adminSession, FILE_PATH, writeBinary);

        Binary readBinary = getBinary(adminSession, FILE_PATH);
        StringWriter writer = new StringWriter();
        IOUtils.copy(readBinary.getStream(), writer, "utf-8");
        assertEquals(buffer, writer.toString());
    }

    // F8 - test reading getBinary().getInputStream() once uploaded
    @Test
    public void testStreamBinaryThroughJCRAfterURIWrite() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        // 1. add binary and upload
        String content = getRandomString(256);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 10);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary binaryWrite = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        saveFileWithBinary(adminSession, FILE_PATH, binaryWrite);

        // 2. stream through JCR and validate it's the same
        Session session = createAdminSession();
        try {
            Binary binaryRead = getBinary(session, FILE_PATH);
            StringWriter writer = new StringWriter();
            IOUtils.copy(binaryRead.getStream(), writer, "utf-8");
            assertEquals(content, writer.toString());
        } finally {
            session.logout();
        }
    }

    // F10 - GET Binary when created via repo
    @Test
    public void testGetBinary() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        // Must be larger than the minimum file size, to keep it from being inlined in the node store.
        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(adminSession, FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        Assert.assertTrue(writeBinary instanceof BinaryDownload);

        URI downloadURI = ((BinaryDownload) writeBinary).getURI(BinaryDownloadOptions.DEFAULT);
        StringWriter writer = new StringWriter();
        IOUtils.copy(httpGet(downloadURI), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // F9 - GET Binary for binary after write using direct PUT
    @Test
    public void testGetBinaryAfterPut() throws Exception {
        // enable writable and readable URI feature
        ConfigurableDataRecordDirectAccessProvider provider = getConfigurableHttpDataRecordProvider();
        provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        // 1. add binary and upload
        String content = getRandomString(256);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 10);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary writeBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());

        // 2. read binary, get the URI
        URI downloadURI = ((BinaryDownload)(writeBinary)).getURI(BinaryDownloadOptions.DEFAULT);
        StringWriter writer = new StringWriter();
        IOUtils.copy(httpGet(downloadURI), writer, "utf-8");

        // 3. GET on URI and verify contents are the same
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetSmallBinaryReturnsNull() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        // Must be smaller than the minimum file size, (inlined binary)
        String content = getRandomString(256);
        Binary writeBinary = createFileWithBinary(adminSession, FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        URI downloadURI = ((BinaryDownload)(writeBinary)).getURI(BinaryDownloadOptions.DEFAULT);
        assertNull(downloadURI);
    }

    @Test
    public void testGetBinaryWithSpecificMediaType() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        String expectedMediaType = "image/png";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withMediaType(expectedMediaType)
                .build();
        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        assertNotNull(mediaType);
        assertEquals(expectedMediaType, mediaType);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetBinaryWithSpecificMediaTypeAndEncoding() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        String expectedMediaType = "text/plain";
        String expectedCharacterEncoding = "utf-8";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withMediaType(expectedMediaType)
                .withCharacterEncoding(expectedCharacterEncoding)
                .build();
        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        assertNotNull(mediaType);
        assertEquals(String.format("%s; charset=%s", expectedMediaType, expectedCharacterEncoding),
                mediaType);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetBinaryWithCharacterEncodingOnly() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        String expectedCharacterEncoding = "utf-8";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withCharacterEncoding(expectedCharacterEncoding)
                .build();
        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        // application/octet-stream is the default Content-Type if none is
        // set in the signed URI
        assertEquals("application/octet-stream", mediaType);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetBinaryWithSpecificFileName() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        String expectedName = "beautiful landscape.png";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withFileName(expectedName)
                .build();
        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String contentDisposition = conn.getHeaderField("Content-Disposition");
        assertNotNull(contentDisposition);
        String encodedName = new String(expectedName.getBytes(StandardCharsets.UTF_8));
        assertEquals(
                String.format("inline; filename=\"%s\"; filename*=UTF-8''%s",
                        expectedName, encodedName),
                contentDisposition
        );

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetBinaryWithSpecificFileNameAndDispositionType() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        String expectedName = "beautiful landscape.png";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withFileName(expectedName)
                .withDispositionTypeAttachment()
                .build();
        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String contentDisposition = conn.getHeaderField("Content-Disposition");
        assertNotNull(contentDisposition);
        String encodedName = new String(expectedName.getBytes(StandardCharsets.UTF_8));
        assertEquals(
                String.format("attachment; filename=\"%s\"; filename*=UTF-8''%s",
                        expectedName, encodedName),
                contentDisposition
        );

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetBinaryWithDispositionType() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withDispositionTypeInline()
                .build();
        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String contentDisposition = conn.getHeaderField("Content-Disposition");
        // Should be no header since filename was not set and disposition type
        // is "inline"
        assertNull(contentDisposition);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());

        downloadOptions = BinaryDownloadOptions
                .builder()
                .withDispositionTypeAttachment()
                .build();
        downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        contentDisposition = conn.getHeaderField("Content-Disposition");
        // Content-Disposition should exist now because "attachment" was set
        assertNotNull(contentDisposition);
        assertEquals("attachment", contentDisposition);
    }

    @Test
    public void testGetBinarySetsAllHeaders() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        String expectedMediaType = "image/png";
        String expectedCharacterEncoding = "utf-8";
        String expectedName = "beautiful landscape.png";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withMediaType(expectedMediaType)
                .withCharacterEncoding(expectedCharacterEncoding)
                .withFileName(expectedName)
                .withDispositionTypeAttachment()
                .build();
        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        assertNotNull(mediaType);
        assertEquals(
                String.format("%s; charset=%s", expectedMediaType, expectedCharacterEncoding),
                mediaType);

        String contentDisposition = conn.getHeaderField("Content-Disposition");
        assertNotNull(contentDisposition);
        String encodedName = new String(expectedName.getBytes(StandardCharsets.UTF_8));
        assertEquals(
                String.format("attachment; filename=\"%s\"; filename*=UTF-8''%s",
                        expectedName, encodedName),
                contentDisposition
        );

        String cacheControl = conn.getHeaderField("Cache-Control");
        assertNotNull(cacheControl);
        assertEquals(String.format("private, max-age=%d, immutable", REGULAR_READ_EXPIRY), cacheControl);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetBinaryDefaultOptions() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH,
                new ByteArrayInputStream(content.getBytes()));

        URI downloadURI = ((BinaryDownload)(writeBinary))
                .getURI(BinaryDownloadOptions.DEFAULT);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        assertNotNull(mediaType);
        assertEquals("application/octet-stream", mediaType);

        String contentDisposition = conn.getHeaderField("Content-Disposition");
        assertNull(contentDisposition);

        String cacheControl = conn.getHeaderField("Cache-Control");
        assertNotNull(cacheControl);
        assertEquals(String.format("private, max-age=%d, immutable", REGULAR_READ_EXPIRY), cacheControl);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // A6 - Client MUST only get permission to add a blob referenced in a JCR binary property
    //      where the user has JCR set_property permission.
    @Test
    @Ignore("OAK-7602")  // michid FIXME OAK-7602
    public void testUnprivilegedSessionCannotUploadBinary() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        try {
            anonymousUploadProvider.initiateBinaryUpload(1024*20, 10);
            fail();
        }
        catch (AccessDeniedException e) { }
    }

    // A2 - disable write URIs entirely
    @Test
    public void testDisableDirectHttpUpload() throws Exception {
        // disable in data store config by setting expiry to zero
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(0);

        String content = getRandomString(256);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 10);

        assertNotNull(upload);
        assertFalse(upload.getUploadURIs().iterator().hasNext());
    }

    // A2 - disable get URIs entirely
    @Test
    public void testDisableDirectHttpDownload() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(0);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(adminSession, FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        URI downloadURI = ((BinaryDownload)(writeBinary)).getURI(BinaryDownloadOptions.DEFAULT);
        assertNull(downloadURI);
    }

    // A2/A3 - configure short expiry time, wait, ensure upload fails after expired
    @Test
    public void testPutURIExpires() throws Exception {
        // short timeout
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(1);

        String content = getRandomString(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 10);

        // wait to pass timeout
        Thread.sleep(2 * SECONDS);

        // ensure PUT fails with 403 or anything 400+
        assertTrue(httpPutTestStream(upload.getUploadURIs().iterator().next()) > HttpURLConnection.HTTP_BAD_REQUEST);
    }

    // F2 - CDN & transfer accelerators (S3 only for now)
    @Test
    public void testTransferAcceleration() throws Exception {
        ConfigurableDataRecordDirectAccessProvider provider = getConfigurableHttpDataRecordProvider();
        if (provider instanceof S3DataStore) {
            // This test is S3 specific for now
            provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
            provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);
            provider.setBinaryTransferAccelerationEnabled(true);

            BinaryUpload upload = uploadProvider.initiateBinaryUpload(1024 * 20, 1);
            URI uri = upload.getUploadURIs().iterator().next();
            assertNotNull(uri);

            LOG.info("accelerated URI: {}", uri.toString());
            assertTrue(uri.getHost().endsWith(".s3-accelerate.amazonaws.com"));

            provider.setBinaryTransferAccelerationEnabled(false);
            upload = uploadProvider.initiateBinaryUpload(1024*20, 1);
            uri = upload.getUploadURIs().iterator().next();
            assertNotNull(uri);

            LOG.info("non-accelerated URI: {}", uri.toString());
            assertFalse(uri.getHost().endsWith(".s3-accelerate.amazonaws.com"));
        }
    }

    // A1 - get put URI, change it and try uploading it
    @Test
    public void testModifiedPutURIFails() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        String content = getRandomString(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 1);
        URI uri = upload.getUploadURIs().iterator().next();
        URI changedURI = new URI(
                String.format("%s://%s/%sX?%s",  // NOTE the injected "X" in the URI filename
                uri.getScheme(),
                uri.getHost(),
                uri.getPath(),
                uri.getQuery())
        );
        int code = httpPut(changedURI, content.getBytes().length, new ByteArrayInputStream(content.getBytes()));
        assertTrue(isFailedHttpPut(code));
    }

    // A1 - get put URI, upload, then try reading from the same URI
    @Test
    public void testCannotReadFromPutURI() throws Exception {
        // enable writable URI and readable URI feature
        ConfigurableDataRecordDirectAccessProvider provider = getConfigurableHttpDataRecordProvider();
        provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 1);
        URI uri = upload.getUploadURIs().iterator().next();
        int code = httpPut(uri, content.getBytes().length, new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        code = conn.getResponseCode();
        assertTrue(isFailedHttpPut(code));
    }

    // A1 - add binary via JCR, then get put URI, and modify to try to upload over first binary
    @Test
    public void testCannotModifyExistingBinaryViaPutURI() throws Exception {
        // enable writable URI and readable URI feature
        ConfigurableDataRecordDirectAccessProvider provider = getConfigurableHttpDataRecordProvider();
        provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(adminSession, FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        URI downloadURI = ((BinaryDownload)(writeBinary)).getURI(BinaryDownloadOptions.DEFAULT);
        assertNotNull(downloadURI);

        String moreContent = getRandomString(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(moreContent.getBytes().length, 1);
        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        IOUtils.copy(new ByteArrayInputStream(moreContent.getBytes()), conn.getOutputStream());

        int code = conn.getResponseCode();
        assertTrue(isFailedHttpPut(code));

        StringWriter writer = new StringWriter();
        IOUtils.copy(httpGet(downloadURI), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // D1 - immutable after initial upload
    @Test
    public void testUploadedBinaryIsImmutable() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        String content = getRandomString(1024*20);

        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 1);
        assertNotNull(upload);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary uploadedBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        saveFileWithBinary(adminSession, FILE_PATH, uploadedBinary);

        Binary binary1 = getBinary(adminSession, FILE_PATH);

        String moreContent = getRandomString(1024*21);
        upload = uploadProvider.initiateBinaryUpload(moreContent.getBytes().length, 1);
        assertNotNull(upload);
        code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(moreContent.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        uploadedBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        saveFileWithBinary(adminSession, FILE_PATH, uploadedBinary);

        Binary binary2 = getBinary(adminSession, FILE_PATH);

        assertTrue(binary1 instanceof ReferenceBinary);
        assertTrue(binary2 instanceof ReferenceBinary);
        assertNotEquals(((ReferenceBinary) binary1).getReference(), ((ReferenceBinary) binary2).getReference());
    }

    // D2 - unique identifiers
    @Test
    public void testUploadPathsAreUnique() throws Exception {
        // Every upload destination should be unique with no regard to file content
        // The content is not read by the Oak code so no deduplication can be performed
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        String content = getRandomString(1024*20);

        BinaryUpload upload1 = uploadProvider.initiateBinaryUpload(content.getBytes().length, 1);
        assertNotNull(upload1);
        BinaryUpload upload2 = uploadProvider.initiateBinaryUpload(content.getBytes().length, 1);
        assertNotNull(upload2);

        assertNotEquals(upload1.getUploadURIs().iterator().next().toString(),
                upload2.getUploadURIs().iterator().next().toString());
    }

    // D3 - do not delete directly => copy nt:file node, delete one, ensure binary still there
    @Test
    public void testBinaryNotDeletedWithNode() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        String content = getRandomString(1024*20);

        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 1);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary binary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        saveFileWithBinary(adminSession, FILE_PATH+"2", binary);

        saveFileWithBinary(adminSession, FILE_PATH, binary);

        adminSession.getNode(FILE_PATH+"2").remove();
        adminSession.save();

        Binary savedBinary = getBinary(adminSession, FILE_PATH);
        assertNotNull(savedBinary);
        assertTrue(binary instanceof ReferenceBinary);
        assertTrue(savedBinary instanceof ReferenceBinary);
        assertEquals(((ReferenceBinary) binary).getReference(),
                ((ReferenceBinary) savedBinary).getReference());
    }

    // D5 - blob ref not persisted in nodestore until binary uploaded and immutable
    @Test
    public void testBinaryOnlyPersistedInNodeStoreAfterUploadIsCompleted() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        String content = getRandomString(1024*20);

        getOrCreateNtFile(adminSession, FILE_PATH);

        Binary binary;
        try {
            binary = getBinary(adminSession, FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.getBytes().length, 1);

        try {
            binary = getBinary(adminSession, FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        try {
            binary = getBinary(adminSession, FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        Binary uploadedBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());

        try {
            binary = getBinary(adminSession, FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        putBinary(adminSession, FILE_PATH, uploadedBinary);

        binary = getBinary(adminSession, FILE_PATH);
        assertNotNull(binary);

        assertTrue(binary instanceof ReferenceBinary);
        assertTrue(uploadedBinary instanceof ReferenceBinary);
        assertEquals(((ReferenceBinary) uploadedBinary).getReference(),
                ((ReferenceBinary) binary).getReference());
    }

    @Test
    public void testInitiateHttpUploadWithZeroSizeFails() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        try {
            uploadProvider.initiateBinaryUpload(0, 1);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateHttpUploadWithZeroURIsFails() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        try {
            uploadProvider.initiateBinaryUpload(1024 * 20, 0);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateHttpUploadWithUnsupportedNegativeNumberURIsFails()
        throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        try {
            uploadProvider.initiateBinaryUpload(1024 * 20, -2);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateHttpUploadWithUnlimitedURIs() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(1024 * 1024 * 1024, -1);
        assertNotNull(upload);
        assertTrue(Iterables.size(upload.getUploadURIs()) > 50);
        // 50 is our default expected client max -
        // this is to make sure we will give as many as needed
        // if the client doesn't specify their own limit
    }

    @Test
    public void testInitiateHttpUploadLargeSinglePut() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(1024 * 1024 * 100, 1);
        assertNotNull(upload);
        assertEquals(1, Iterables.size(upload.getUploadURIs()));
    }

    @Test
    public void testInitiateHttpUploadTooLargeForSinglePut() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        try {
            uploadProvider.initiateBinaryUpload(1024L * 1024L * 1024L * 10L, 1);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateHttpUploadTooLargeForUpload() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        try {
            uploadProvider.initiateBinaryUpload(1024L * 1024L * 1024L * 1024L * 10L, -1);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testInitiateHttpUploadTooLargeForRequestedNumURIs() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        try {
            uploadProvider.initiateBinaryUpload(1024L * 1024L * 1024L * 10L, 10);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testCompleteHttpUploadWithInvalidTokenFails() throws RepositoryException {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        BinaryUpload upload = uploadProvider.initiateBinaryUpload(256, 10);
        assertNotNull(upload);

        try {
            uploadProvider.completeBinaryUpload(upload.getUploadToken() + "X");
            fail();
        }
        catch (IllegalArgumentException e) { }
    }
}
