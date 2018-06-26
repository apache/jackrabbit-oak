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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Random;

import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.oak.api.blob.IllegalHttpUploadArgumentsException;
import org.apache.jackrabbit.oak.api.blob.InvalidHttpUploadTokenException;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.api.binary.HttpBinaryUpload;
import org.apache.jackrabbit.oak.jcr.api.binary.HttpBinaryProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.ConfigurableHttpDataRecordProvider;
import org.junit.Before;
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
    private static final String BINARY_PATH = Joiner.on("/").join(
            FILE_PATH,
            JcrConstants.JCR_CONTENT,
            JcrConstants.JCR_DATA
    );
    private static final int REGULAR_WRITE_EXPIRY = 60; // seconds
    private static final int REGULAR_READ_EXPIRY = 60; // seconds

    public HttpBinaryIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void cleanRepoContents() throws RepositoryException {
        Session adminSession = getAdminSession();
        if (adminSession.nodeExists(FILE_PATH)) {
            adminSession.getNode(FILE_PATH).remove();
            adminSession.save();
        }
    }

    // F1 - basic test
    @Test
    public void testUpload() throws Exception {
        enableUpload();

        final String content = getRandomString(256);
        final long size = content.getBytes("utf-8").length;

        assertTrue(getAdminSession() instanceof HttpBinaryProvider);

        HttpBinaryProvider uploadProvider = (HttpBinaryProvider) getAdminSession();

        HttpBinaryUpload upload = uploadProvider.initiateHttpUpload(BINARY_PATH, size, 1);
        assertNotNull(upload);

        // very small test binary
        assertTrue(size < upload.getMaxPartSize());

        URI uri = upload.getUploadURIs().iterator().next();
        assertNotNull(uri);

        LOG.info("- uploading binary via PUT to {}", uri.toString());
        int code = httpPut(uri, content.getBytes().length, getTestInputStream(content));

        assertTrue("PUT to pre-signed S3 URL failed",
                isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary writeBinary = uploadProvider.completeHttpUpload(upload.getUploadToken());
        putBinary(getAdminSession(), FILE_PATH, writeBinary);

        Binary readBinary = getBinary(getAdminSession(), FILE_PATH);
        StringWriter writer = new StringWriter();
        IOUtils.copy(readBinary.getStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // F3 - Multi-part upload
    @Test
    public void testMultiPartUpload() throws Exception {
        enableUpload();

        assertTrue(getAdminSession() instanceof HttpBinaryProvider);

        HttpBinaryProvider uploadProvider = (HttpBinaryProvider) getAdminSession();

        // 25MB is a good size to ensure chunking is done
        long uploadSize = 1024 * 1024 * 25;
        HttpBinaryUpload uploadContext = uploadProvider.initiateHttpUpload(BINARY_PATH, uploadSize, 50);
        assertNotNull(uploadContext);

        String buffer = getRandomString(uploadSize);
        long uploadPartSize = uploadContext.getMaxPartSize();
        long remaining = uploadSize;
        for (URI uri : uploadContext.getUploadURIs()) {
            String nextPart = buffer.substring(0, (int) Math.min(uploadPartSize, remaining));
            int code = httpPut(uri,
                    nextPart.getBytes().length,
                    new ByteArrayInputStream(nextPart.getBytes()),
                    true);
            assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
            remaining -= uploadPartSize;
            if (remaining <= 0) break;
        }

        Binary writeBinary = uploadProvider.completeHttpUpload(uploadContext.getUploadToken());
        putBinary(getAdminSession(), FILE_PATH, writeBinary);

        Binary readBinary = getBinary(getAdminSession(), FILE_PATH);
        StringWriter writer = new StringWriter();
        IOUtils.copy(readBinary.getStream(), writer, "utf-8");
        assertEquals(buffer, writer.toString());
    }

    // F8 - test reading getBinary().getInputStream() once uploaded
    @Test
    public void testStreamBinaryThroughJCRAfterURIWrite() throws Exception {
        enableUpload();

        // 1. add binary and upload
        String content = getRandomString(256);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(
                FILE_PATH,
                content.getBytes().length,
                10);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary binaryWrite = ((HttpBinaryProvider) getAdminSession()).completeHttpUpload(upload.getUploadToken());
        saveFileWithBinary(getAdminSession(), FILE_PATH, binaryWrite);

        // 2. stream through JCR and validate it's the same
        Binary binaryRead = getBinary(createAdminSession(), FILE_PATH);
        StringWriter writer = new StringWriter();
        IOUtils.copy(binaryRead.getStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // F10 - GET Binary when created via repo
    @Test
    public void testGetBinary() throws Exception {
        enableDownload();

        // Must be larger than the minimum file size, to keep it from being inlined in the node store.
        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        waitForUploads();

        URI downloadURI = ((HttpBinaryProvider) getAdminSession()).getHttpDownloadURI(writeBinary);
        StringWriter writer = new StringWriter();
        IOUtils.copy(httpGet(downloadURI), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // F9 - GET Binary for binary after write using direct PUT
    @Test
    public void testGetBinaryAfterPut() throws Exception {
        enableUpload();
        enableDownload();

        // 1. add binary and upload
        String content = getRandomString(256);
        HttpBinaryProvider binaryProvider = (HttpBinaryProvider) getAdminSession();
        HttpBinaryUpload upload = binaryProvider.initiateHttpUpload(BINARY_PATH, content.getBytes().length, 10);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary writeBinary = binaryProvider.completeHttpUpload(upload.getUploadToken());

        // 2. read binary, get the URI
        URI downloadURI = binaryProvider.getHttpDownloadURI(writeBinary);
        StringWriter writer = new StringWriter();
        IOUtils.copy(httpGet(downloadURI), writer, "utf-8");

        // 3. GET on URI and verify contents are the same
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetSmallBinaryReturnsNull() throws Exception {
        enableUpload();

        // Must be smaller than the minimum file size, (inlined binary)
        String content = getRandomString(256);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        waitForUploads();

        URI downloadURI = ((HttpBinaryProvider) getAdminSession()).getHttpDownloadURI(writeBinary);
        assertNull(downloadURI);
    }

    // A6 - Client MUST only get permission to add a blob referenced in a JCR binary property
    //      where the user has JCR set_property permission.
    @Test
    public void testUnprivilegedSessionCannotUploadBinary() throws Exception {
        enableUpload();

        try {
            ((HttpBinaryProvider) getAnonymousSession()).initiateHttpUpload(BINARY_PATH, 1024*20, 10);
            fail();
        }
        catch (AccessDeniedException e) { }
    }

    // A2 - disable write URIs entirely
    @Test
    public void testDisableDirectHttpUpload() throws Exception {
        disableUpload();

        String content = getRandomString(256);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 10);

        assertNotNull(upload);
        assertEquals(0, upload.getUploadURIs().size());
    }

    // A2 - disable get URIs entirely
    @Test
    public void testDisableDirectHttpDownload() throws Exception {
        disableDownload();

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        waitForUploads();

        URI downloadURI = ((HttpBinaryProvider) getAdminSession()).getHttpDownloadURI(writeBinary);
        assertNull(downloadURI);
    }

    // A2/A3 - configure short expiry time, wait, ensure upload fails after expired
    @Test
    public void testPutURIExpires() throws Exception {
        // short timeout
        configureUpload(1);

        String content = getRandomString(1024*20);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 10);

        // wait to pass timeout
        Thread.sleep(2 * SECONDS);

        // ensure PUT fails with 403 or anything 400+
        assertTrue(httpPutTestStream(upload.getUploadURIs().iterator().next()) > HttpURLConnection.HTTP_BAD_REQUEST);
    }

    // F2 - CDN & transfer accelerators (S3 only for now)
    @Test
    public void testTransferAcceleration() throws Exception {
        ConfigurableHttpDataRecordProvider provider = getConfigurableHttpDataRecordProvider();
        if (provider instanceof S3DataStore) {
            // This test is S3 specific for now
            enableUpload();
            enableDownload();
            provider.setBinaryTransferAccelerationEnabled(true);

            HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH,1024*20, 1);
            URI uri = upload.getUploadURIs().iterator().next();
            assertNotNull(uri);

            LOG.info("accelerated URL: {}", uri.toString());
            assertTrue(uri.getHost().endsWith(".s3-accelerate.amazonaws.com"));

            provider.setBinaryTransferAccelerationEnabled(false);
            upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH,1024*20, 1);
            uri = upload.getUploadURIs().iterator().next();
            assertNotNull(uri);

            LOG.info("non-accelerated URL: {}", uri.toString());
            assertFalse(uri.getHost().endsWith(".s3-accelerate.amazonaws.com"));
        }
    }

    // A1 - get put URI, change it and try uploading it
    @Test
    public void testModifiedPutURIFails() throws Exception {
        enableUpload();
        String content = getRandomString(1024*20);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 1);
        URI uri = upload.getUploadURIs().iterator().next();
        URI changedUri = new URI(
                String.format("%s://%s/%sX?%s",  // NOTE the injected "X" in the URL filename
                        uri.getScheme(),
                        uri.getHost(),
                        uri.getPath(),
                        uri.getQuery())
        );
        int code = httpPut(changedUri, content.getBytes().length, new ByteArrayInputStream(content.getBytes()));
        assertTrue(isFailedHttpPut(code));
    }

    // A1 - get put uri, upload, then try reading from the same uri
    @Test
    public void testCannotReadFromPutURI() throws Exception {
        enableUpload();
        enableDownload();

        String content = getRandomString(1024*20);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 1);
        URI uri = upload.getUploadURIs().iterator().next();
        int code = httpPut(uri, content.getBytes().length, new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        code = conn.getResponseCode();
        assertTrue(isFailedHttpPut(code));
    }

    // A1 - add binary via JCR, then get put uri, and modify to try to upload over first binary
    @Test
    public void testCannotModifyExistingBinaryViaPutURI() throws Exception {
        enableUpload();
        enableDownload();

        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        waitForUploads();

        URI downloadURI = ((HttpBinaryProvider) getAdminSession()).getHttpDownloadURI(writeBinary);
        assertNotNull(downloadURI);

        String moreContent = getRandomString(1024*20);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, moreContent.getBytes().length, 1);
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
        enableUpload();

        String content = getRandomString(1024*20);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 1);
        assertNotNull(upload);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary uploadedBinary = ((HttpBinaryProvider) getAdminSession()).completeHttpUpload(upload.getUploadToken());
        saveFileWithBinary(getAdminSession(), FILE_PATH, uploadedBinary);

        Binary binary1 = getBinary(getAdminSession(), FILE_PATH);

        String moreContent = getRandomString(1024*21);
        upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, moreContent.getBytes().length, 1);
        assertNotNull(upload);
        code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(moreContent.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        uploadedBinary = ((HttpBinaryProvider) getAdminSession()).completeHttpUpload(upload.getUploadToken());
        saveFileWithBinary(getAdminSession(), FILE_PATH, uploadedBinary);

        Binary binary2 = getBinary(getAdminSession(), FILE_PATH);

        assertTrue(binary1 instanceof ReferenceBinary);
        assertTrue(binary2 instanceof ReferenceBinary);
        assertNotEquals(((ReferenceBinary) binary1).getReference(), ((ReferenceBinary) binary2).getReference());
    }

    // D2 - unique identifiers
    @Test
    public void testUploadPathsAreUnique() throws Exception {
        // Every upload destination should be unique with no regard to file content
        // The content is not read by the Oak code so no deduplication can be performed

        enableUpload();

        String content = getRandomString(1024*20);
        HttpBinaryUpload upload1 = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 1);
        assertNotNull(upload1);
        HttpBinaryUpload upload2 = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 1);
        assertNotNull(upload2);

        assertNotEquals(upload1.getUploadURIs().iterator().next().toString(),
                upload2.getUploadURIs().iterator().next().toString());
    }

    // D3 - do not delete directly => copy nt:file node, delete one, ensure binary still there
    @Test
    public void testBinaryNotDeletedWithNode() throws Exception {
        enableUpload();

        String content = getRandomString(1024*20);
        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 1);
        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary binary = ((HttpBinaryProvider) getAdminSession()).completeHttpUpload(upload.getUploadToken());
        saveFileWithBinary(getAdminSession(), FILE_PATH+"2", binary);

        saveFileWithBinary(getAdminSession(), FILE_PATH, binary);

        getAdminSession().getNode(FILE_PATH+"2").remove();
        getAdminSession().save();

        Binary savedBinary = getBinary(getAdminSession(), FILE_PATH);
        assertNotNull(savedBinary);
        assertTrue(binary instanceof ReferenceBinary);
        assertTrue(savedBinary instanceof ReferenceBinary);
        assertEquals(((ReferenceBinary) binary).getReference(),
                ((ReferenceBinary) savedBinary).getReference());
    }

    // D5 - blob ref not persisted in nodestore until binary uploaded and immutable
    @Test
    public void testBinaryOnlyPersistedInNodeStoreAfterUploadIsCompleted() throws Exception {
        enableUpload();

        String content = getRandomString(1024*20);

        getOrCreateNtFile(getAdminSession(), FILE_PATH);

        Binary binary;
        try {
            binary = getBinary(getAdminSession(), FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, content.getBytes().length, 1);

        try {
            binary = getBinary(getAdminSession(), FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        int code = httpPut(upload.getUploadURIs().iterator().next(),
                content.getBytes().length,
                new ByteArrayInputStream(content.getBytes()));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        try {
            binary = getBinary(getAdminSession(), FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        Binary uploadedBinary = ((HttpBinaryProvider) getAdminSession()).completeHttpUpload(upload.getUploadToken());

        try {
            binary = getBinary(getAdminSession(), FILE_PATH);
            fail();
        }
        catch (PathNotFoundException e) { }

        putBinary(getAdminSession(), FILE_PATH, uploadedBinary);

        binary = getBinary(getAdminSession(), FILE_PATH);
        assertNotNull(binary);

        assertTrue(binary instanceof ReferenceBinary);
        assertTrue(uploadedBinary instanceof ReferenceBinary);
        assertEquals(((ReferenceBinary) uploadedBinary).getReference(),
                ((ReferenceBinary) binary).getReference());
    }

    @Test
    public void testInitiateHttpUploadWithZeroSizeFails() throws RepositoryException {
        enableUpload();

        try {
            ((HttpBinaryProvider) getAdminSession())
                    .initiateHttpUpload(BINARY_PATH,0, 1);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalHttpUploadArgumentsException);
        }
    }

    @Test
    public void testInitiateHttpUploadWithZeroURIsFails() throws RepositoryException {
        enableUpload();

        try {
            ((HttpBinaryProvider) getAdminSession()).initiateHttpUpload(BINARY_PATH, 1024 * 20, 0);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalHttpUploadArgumentsException);
        }
    }

    @Test
    public void testInitiateHttpUploadWithUnlimitedURIs() throws RepositoryException {
        enableUpload();

        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession())
                .initiateHttpUpload(BINARY_PATH,1024 * 1024 * 1024, -1);
        assertNotNull(upload);
        assertTrue(upload.getUploadURIs().size() > 50);
        // 50 is our default expected client max -
        // this is to make sure we will give as many as needed
        // if the client doesn't specify their own limit
    }

    @Test
    public void testInitiateHttpUploadLargeSinglePut() throws RepositoryException {
        enableUpload();

        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession())
                .initiateHttpUpload(BINARY_PATH,1024 * 1024 * 100, 1);
        assertNotNull(upload);
        assertEquals(1, upload.getUploadURIs().size());
    }

    @Test
    public void testInitiateHttpUploadTooLargeForSinglePut() throws RepositoryException {
        enableUpload();

        try {
            ((HttpBinaryProvider) getAdminSession())
                    .initiateHttpUpload(BINARY_PATH,1024L * 1024L * 1024L * 10L, 1);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalHttpUploadArgumentsException);
        }
    }

    @Test
    public void testInitiateHttpUploadTooLargeForUpload() throws RepositoryException {
        enableUpload();

        try {
            ((HttpBinaryProvider) getAdminSession())
                    .initiateHttpUpload(BINARY_PATH, 1024L * 1024L * 1024L * 1024L * 10L, -1);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalHttpUploadArgumentsException);
        }
    }

    @Test
    public void testInitiateHttpUploadTooLargeForRequestedNumURIs() throws RepositoryException {
        enableUpload();

        try {
            ((HttpBinaryProvider) getAdminSession())
                    .initiateHttpUpload(BINARY_PATH, 1024L * 1024L * 1024L * 10L, 10);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalHttpUploadArgumentsException);
        }
    }

    @Test
    public void testCompleteHttpUploadWithInvalidTokenFails() throws RepositoryException {
        enableUpload();

        HttpBinaryUpload upload = ((HttpBinaryProvider) getAdminSession())
                .initiateHttpUpload(BINARY_PATH, 256, 10);
        assertNotNull(upload);
        try {
            ((HttpBinaryProvider) getAdminSession()).completeHttpUpload(upload.getUploadToken()+"X");
            fail();
        }
        catch (InvalidHttpUploadTokenException e) { }
    }

    // -----------------------------------------------------------------< helpers >--------------

    private ConfigurableHttpDataRecordProvider enableUpload() throws RepositoryException {
        return configureUpload(REGULAR_WRITE_EXPIRY);
    }

    private ConfigurableHttpDataRecordProvider disableUpload() throws RepositoryException {
        return configureUpload(0);
    }

    private ConfigurableHttpDataRecordProvider configureUpload(int expiry) throws RepositoryException {
        ConfigurableHttpDataRecordProvider provider =
                getConfigurableHttpDataRecordProvider();
        provider.setHttpUploadURIExpirySeconds(expiry);
        return provider;
    }

    private ConfigurableHttpDataRecordProvider enableDownload() throws RepositoryException {
        return configureDownload(REGULAR_READ_EXPIRY);
    }

    private ConfigurableHttpDataRecordProvider disableDownload() throws RepositoryException {
        return configureDownload(0);
    }

    private ConfigurableHttpDataRecordProvider configureDownload(int expiry) throws RepositoryException {
        ConfigurableHttpDataRecordProvider provider =
                getConfigurableHttpDataRecordProvider();
        provider.setHttpDownloadURIExpirySeconds(expiry);
        return provider;
    }

    private Node getOrCreateNtFile(Session session, String path) throws RepositoryException {
        if (session.nodeExists(path + "/" + JcrConstants.JCR_CONTENT)) {
            return session.getNode(path + "/" + JcrConstants.JCR_CONTENT);
        }
        Node file = session.getRootNode().addNode(path.substring(1), JcrConstants.NT_FILE);
        return file.addNode(JcrConstants.JCR_CONTENT, JcrConstants.NT_RESOURCE);
    }

    private void putBinary(Session session, String ntFilePath, Binary binary) throws RepositoryException {
        Node node = getOrCreateNtFile(session, ntFilePath);
        node.setProperty(JcrConstants.JCR_DATA, binary);
    }

    private Binary getBinary(Session session, String ntFilePath) throws RepositoryException {
        return session.getNode(ntFilePath)
            .getNode(JcrConstants.JCR_CONTENT)
            .getProperty(JcrConstants.JCR_DATA)
            .getBinary();
    }

    private String getRandomString(long size) {
        String base = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ+/";
        StringWriter writer = new StringWriter();
        Random random = new Random();
        if (size > 256) {
            String str256 = getRandomString(256);
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
                writer.append(base.charAt(random.nextInt(base.length())));
            }
        }
        return writer.toString();
    }

    /** Saves an nt:file with a binary at the given path and saves the session. */
    private void saveFileWithBinary(Session session, String path, Binary binary) throws RepositoryException {
        Node node = getOrCreateNtFile(session, path);
        node.setProperty(JcrConstants.JCR_DATA, binary);
        session.save();
    }

    private Binary createFileWithBinary(Session session, String path, InputStream stream) throws RepositoryException {
        Binary binary = session.getValueFactory().createBinary(stream);
        saveFileWithBinary(session, path, binary);
        return binary;
    }

}
