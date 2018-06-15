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

package org.apache.jackrabbit.oak.jcr.binary;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.api.binary.BinaryHttpUpload;
import org.apache.jackrabbit.oak.jcr.api.binary.HttpBinaryProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.ConfigurableHttpDataRecordProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration test for direct binary GET/PUT via HTTP, that requires a fully working data store
 * (such as S3) for each {@link AbstractHttpBinaryIT#dataStoreFixtures() configured fixture}.
 * The data store in question must support direct GET/PUT access via a URL.
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
    private static final String CONTENT = "hi there, I'm a binary blob!";
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

    // TODO: one test for each requirement
    // F2 - CDN & transfer accelerators
    // F4 - S3 and Azure => through parametrization using S3 and Azure fixtures
    // F5 - more cloud stores => new fixtures, mock fixture
    // F6/F7 - no additional final request, notification API via SQS

    // A1 - get put url, change it and try uploading somewhere else in S3
    // A4 - no test, SHOULD requirement only, hard to test
    // A5 - get S3 URL (how?) and try an upload

    // D1 - immutable after initial upload
    // D2 - unique identifiers
    // D3 - do not delete directly => copy nt:file node, delete one, ensure binary still there
    // D5 - blob ref not persisted in nodestore until binary uploaded and immutable
    // DX - get existing regular binary and try to overwrite it (doesn't work)

    // F1 - basic test
    @Test
    public void testUpload() throws Exception {
        // enable writable URL feature
        getConfigurableHttpDataRecordProvider()
                .setHttpUploadURLExpirySeconds(REGULAR_WRITE_EXPIRY);

        final String TEST_BINARY = "hello world";
        final long size = TEST_BINARY.getBytes("utf-8").length;

        assertTrue(getAdminSession() instanceof HttpBinaryProvider);

        HttpBinaryProvider uploadProvider = (HttpBinaryProvider) getAdminSession();

        BinaryHttpUpload upload = uploadProvider.initializeHttpUpload(size, 1);
        assertNotNull(upload);

        // very small test binary
        assertTrue(size < upload.getMaxPartSize());

        URL url = upload.getURLParts().iterator().next();
        assertNotNull(url);

        System.out.println("- uploading binary via PUT to " + url);
        int code = httpPut(url, getTestInputStream(TEST_BINARY));

        assertEquals("PUT to pre-signed S3 URL failed", 200, code);

        Binary writeBinary = uploadProvider.completeHttpUpload(upload.getUploadToken());
        putBinary(getAdminSession(), FILE_PATH, writeBinary);

        Binary readBinary = getBinary(getAdminSession(), FILE_PATH);
        StringWriter writer = new StringWriter();
        IOUtils.copy(readBinary.getStream(), writer, "utf-8");
        assertEquals(TEST_BINARY, writer.toString());
    }

    // F3 - Multi-part upload
    @Test
    public void testMultiPartUpload() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setHttpUploadURLExpirySeconds(REGULAR_WRITE_EXPIRY);

        assertTrue(getAdminSession() instanceof HttpBinaryProvider);

        HttpBinaryProvider uploadProvider = (HttpBinaryProvider) getAdminSession();

        // 25MB is a good size to ensure chunking is done
        long uploadSize = 1024 * 1024 * 25;
        BinaryHttpUpload uploadContext = uploadProvider.initializeHttpUpload(uploadSize, 50);
        assertNotNull(uploadContext);

        String buffer = getRandomString(uploadSize);
        long uploadPartSize = uploadContext.getMaxPartSize();
        long remaining = uploadSize;
        for (URL url : uploadContext.getURLParts()) {
            int code = httpPut(url, new ByteArrayInputStream(buffer.substring(0, (int) Math.min(uploadPartSize, remaining)).getBytes()));
            assertEquals("PUT to pre-signed URL failed", 200, code);
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

    // D1/S2 - test reading getBinary().getInputStream() once uploaded
//    @Test
//    public void testWriteURLDoesNotChange() throws Exception {
//        // enable writable URL feature
//        getConfigurableHttpDataRecordProvider()
//                .setHttpUploadURLExpirySeconds(REGULAR_WRITE_EXPIRY);
//
//        // 1. add binary
//        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
//
//        // 2. request url for the 1st time (and check it's not null)
//        URL url = binary.getWriteURL();
//        assertNotNull(url);
//
//        // 3. assert that 2nd request yields the exact same URL
//        assertEquals(url, binary.getWriteURL());
//    }
//
    // F8 - test reading getBinary().getInputStream() once uploaded
    @Test
    public void testStreamBinaryThroughJCRAfterURLWrite() throws Exception {
        // enable writable URL feature
        getConfigurableHttpDataRecordProvider()
                .setHttpUploadURLExpirySeconds(REGULAR_WRITE_EXPIRY);

        // 1. add binary and upload
        String content = getRandomString(256);
        BinaryHttpUpload upload = ((HttpBinaryProvider) getAdminSession()).initializeHttpUpload(
                content.getBytes().length, 10);
        httpPut(upload.getURLParts().iterator().next(), new ByteArrayInputStream(content.getBytes()));
        Binary binaryWrite = ((HttpBinaryProvider) getAdminSession()).completeHttpUpload(upload.getUploadToken());
        saveFileWithBinary(getAdminSession(), FILE_PATH, binaryWrite);

        // 2. stream through JCR and validate it's the same
        Binary binaryRead = getBinary(createAdminSession(), FILE_PATH);
        StringWriter writer = new StringWriter();
        IOUtils.copy(binaryRead.getStream(), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // F9 - GET Binary when created via repo
    @Test
    public void testGetBinary() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setHttpDownloadURLExpirySeconds(REGULAR_READ_EXPIRY);

        // Must be larger than the minimum file size, to keep it from being inlined in the node store.
        String content = getRandomString(1024*20);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        waitForUploads();

        URL downloadURL = ((HttpBinaryProvider) getAdminSession()).getHttpDownloadURL(writeBinary);
        StringWriter writer = new StringWriter();
        IOUtils.copy(httpGet(downloadURL), writer, "utf-8");
        assertEquals(content, writer.toString());
    }

    // F9 - GET Binary for binary after write using direct PUT
    @Test
    public void testGetBinaryAfterPut() throws Exception {
        // enable writable and readable URL feature
        ConfigurableHttpDataRecordProvider provider = getConfigurableHttpDataRecordProvider();
        provider.setHttpUploadURLExpirySeconds(REGULAR_WRITE_EXPIRY);
        provider.setHttpDownloadURLExpirySeconds(REGULAR_READ_EXPIRY);

        // 1. add binary and upload
        String content = getRandomString(256);
        HttpBinaryProvider binaryProvider = (HttpBinaryProvider) getAdminSession();
        BinaryHttpUpload upload = binaryProvider.initializeHttpUpload(content.getBytes().length, 10);
        httpPut(upload.getURLParts().iterator().next(), new ByteArrayInputStream(content.getBytes()));
        Binary writeBinary = binaryProvider.completeHttpUpload(upload.getUploadToken());

        // 2. read binary, get the URL
        URL downloadURL = binaryProvider.getHttpDownloadURL(writeBinary);
        StringWriter writer = new StringWriter();
        IOUtils.copy(httpGet(downloadURL), writer, "utf-8");

        // 3. GET on URL and verify contents are the same
        assertEquals(content, writer.toString());
    }

    @Test
    public void testGetSmallBinaryReturnsNull() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setHttpDownloadURLExpirySeconds(REGULAR_READ_EXPIRY);

        // Must be smaller than the minimum file size, (inlined binary)
        String content = getRandomString(256);
        Binary writeBinary = createFileWithBinary(getAdminSession(), FILE_PATH, new ByteArrayInputStream(content.getBytes()));

        waitForUploads();

        URL downloadURL = ((HttpBinaryProvider) getAdminSession()).getHttpDownloadURL(writeBinary);
        assertNull(downloadURL);
    }

//    // F10 - URLReadableBinary for binary added through InputStream (has to wait for S3 upload)
//    @Test
//    public void testURLReadableBinaryFromInputStream() throws Exception {
//        // enable readable URL feature
//        getConfigurableHttpDataRecordProvider()
//                .setHttpDownloadURLExpirySeconds(REGULAR_READ_EXPIRY);
//
//        // 1. add binary via input stream (must be larger than 16 KB segmentstore inline binary limit)
//        Node resource = getOrCreateNtFile(getAdminSession(), FILE_PATH);
//        Binary streamBinary = getAdminSession().getValueFactory().createBinary(getTestInputStream(MB));
//        resource.setProperty(JcrConstants.JCR_DATA, streamBinary);
//
//        waitForUploads();
//
//        // 2. read binary, check it's a URLReadableBinary and get the URL
//        Binary binary = getBinary(getAdminSession(), FILE_PATH);
//        assertTrue(binary instanceof URLReadableBinary);
//        URL url = ((URLReadableBinary) binary).getReadURL();
//        assertNotNull(url);
//
//        // 3. GET on URL and verify contents are the same
//        InputStream stream = httpGet(url);
//        assertTrue(IOUtils.contentEquals(stream, getTestInputStream(MB)));
//    }

    // A6 - Client MUST only get permission to add a blob referenced in a JCR binary property
    //      where the user has JCR set_property permission.
//    @Test
//    public void testReadingBinaryDoesNotReturnURLWritableBinary() throws Exception {
//        // 1. create URL access binary
//        saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
//
//        // 2. then get existing url access binary using read-only session
//        Binary binary = getBinary(createAnonymousSession(), FILE_PATH);
//
//        // 3. ensure we do not get a writable binary
//        assertFalse(binary instanceof URLWritableBinary);
//    }

    // A2 - disable write URLs entirely
//    @Test
//    public void testDisabledURLWritableBinary() throws Exception {
//        // disable in data store config by setting expiry to zero
//        getConfigurableHttpDataRecordProvider()
//                .setHttpUploadURLExpirySeconds(0);
//
//        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
//        // TODO: we might want to not return a URLWritableBinary in the first place if it's disabled
//        assertNotNull(binary);
//        assertNull(binary.getWriteURL());
//
//        // TODO: extra test, showing alternative input stream code working if disabled
///*
//        if (placeholderBinary == null) {
//            // fallback
//            System.out.println(">>> NO url binary support");
//            // TODO: normally, a client would set an empty binary here and overwrite with an inputstream in a future, 2nd request
//            // generate 2 MB of meaningless bytes
//            placeholderBinary = valueFactory.createBinary(getTestInputStream(2 * MB));
//        }
//*/
//    }

    // A2/A3 - configure short expiry time, wait, ensure upload fails after expired
//    @Test
//    public void testExpiryOfURLWritableBinary() throws Exception {
//        // short timeout
//        getConfigurableHttpDataRecordProvider()
//                .setHttpUploadURLExpirySeconds(1);
//
//        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
//        URL url = binary.getWriteURL();
//
//        // wait to pass timeout
//        Thread.sleep(2 * SECONDS);
//
//        // ensure PUT fails with 403 or anything 400+
//        assertTrue(httpPutTestStream(url) > HttpURLConnection.HTTP_BAD_REQUEST);
//    }

    // this tests an initial unexpected side effect bug where the underlying URLWritableBlob implementation
    // failed to work in a hash set with at least two blobs because of default equals/hashCode methods trying
    // to read the length() of the binary which is not available for to be written URLWritableBlobs
//    @Test
//    public void testMultipleURLWritableBinariesInOneSave() throws Exception {
//        getConfigurableHttpDataRecordProvider()
//                .setHttpUploadURLExpirySeconds(REGULAR_WRITE_EXPIRY);
//
//        URLWritableBinary binary1 = createFileWithURLWritableBinary(getAdminSession(), FILE_PATH, false);
//        URLWritableBinary binary2 = createFileWithURLWritableBinary(getAdminSession(), FILE_PATH + "2", false);
//        getAdminSession().save();
//
//        URL url1 = binary1.getWriteURL();
//        assertNotNull(url1);
//        assertEquals(200, httpPutTestStream(url1));
//        URL url2 = binary2.getWriteURL();
//        assertNotNull(url2);
//        assertEquals(200, httpPutTestStream(url2));
//    }
//
//    @Test
//    public void testURLReadableBinaryCache() throws Exception {
//        ConfigurableHttpDataRecordProvider provider = getConfigurableHttpDataRecordProvider();
//        provider.setHttpDownloadURLExpirySeconds(REGULAR_READ_EXPIRY);
//        provider.setURLReadableBinaryURLCacheSize(100);
//
//        // 1. create URL access binary
//        saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
//
//        // 2. then get url binary twice
//        Binary binary1 = getBinary(getAdminSession(), FILE_PATH);
//        Binary binary2 = getBinary(getAdminSession(), FILE_PATH);
//
//        // test that we actually have different binaries
//        assertFalse(binary1 == binary2);
//        assertTrue(binary1 instanceof URLReadableBinary);
//        assertTrue(binary2 instanceof URLReadableBinary);
//
//        // 3. ensure URLs are the same because caching
//        URL url1 = ((URLReadableBinary) binary1).getReadURL();
//        assertNotNull(url1);
//        URL url2 = ((URLReadableBinary) binary2).getReadURL();
//        assertNotNull(url2);
//        assertEquals(url1, url2);
//
//        // ------------------------------------------------------
//        // turn off cache
//        provider.setURLReadableBinaryURLCacheSize(100); // TODO - should be 0?
//        // getURLReadableDataStore().setURLReadableBinaryURLCacheSize(100);
//
//        binary1 = getBinary(getAdminSession(), FILE_PATH);
//        binary2 = getBinary(getAdminSession(), FILE_PATH);
//
//        // test that we actually have different binaries
//        assertFalse(binary1 == binary2);
//        assertTrue(binary1 instanceof URLReadableBinary);
//        assertTrue(binary2 instanceof URLReadableBinary);
//
//        // 3. ensure URLs are the same because caching
//        url1 = ((URLReadableBinary) binary1).getReadURL();
//        assertNotNull(url1);
//        url2 = ((URLReadableBinary) binary2).getReadURL();
//        assertNotNull(url2);
//        assertEquals(url1, url2);
//    }

    // TODO: this test is S3 specific for now
//    @Test
//    public void testTransferAcceleration() throws Exception {
//        ConfigurableHttpDataRecordProvider provider = getConfigurableHttpDataRecordProvider();
//        provider.setHttpUploadURLExpirySeconds(REGULAR_WRITE_EXPIRY);
//        provider.setHttpDownloadURLExpirySeconds(REGULAR_READ_EXPIRY);
//        provider.setBinaryTransferAccelerationEnabled(true);
//
//        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
//        URL url = binary.getWriteURL();
//        assertNotNull(url);
//
//        System.out.println("accelerated URL: " + url);
//        assertTrue(url.getHost().endsWith(".s3-accelerate.amazonaws.com"));
//
//        provider.setBinaryTransferAccelerationEnabled(false);
//
//        binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
//        url = binary.getWriteURL();
//        assertNotNull(url);
//
//        System.out.println("non-accelerated URL: " + url);
//        assertFalse(url.getHost().endsWith(".s3-accelerate.amazonaws.com"));
//    }

    // disabled, just a comparison playground for current blob behavior
    //@Test
    public void testReferenceBinary() throws Exception {
        Session session = createAdminSession();
        Node file = session.getRootNode().addNode("file");
        file.setProperty("binary", session.getValueFactory().createBinary(getTestInputStream(2 * MB)));
        session.save();

        waitForUploads();

        Binary binary = file.getProperty("binary").getBinary();
        if (binary instanceof ReferenceBinary) {
            ReferenceBinary referenceBinary = (ReferenceBinary) binary;
            String ref = referenceBinary.getReference();
            System.out.println("Ref: " + ref);
            String blobId = ref.substring(0, ref.indexOf(':'));
            System.out.println("blobId: " + blobId);
        }
    }

    // -----------------------------------------------------------------< helpers >--------------

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

//    /** Creates an nt:file with an url access binary at the given path. */
//    @Nonnull
//    private URLWritableBinary createFileWithURLWritableBinary(Session session, String path, boolean autoSave) throws RepositoryException {
//        Node resource = getOrCreateNtFile(session, path);
//
//        ValueFactory valueFactory = session.getValueFactory();
//        assertTrue(valueFactory instanceof URLWritableBinaryValueFactory);
//
//        URLWritableBinary binary = ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
//        assertNotNull("URLWritableBinary not supported", binary);
//        resource.setProperty(JcrConstants.JCR_DATA, binary);
//
//        if (autoSave) {
//            session.save();
//        }
//
//        return binary;
//    }

}
