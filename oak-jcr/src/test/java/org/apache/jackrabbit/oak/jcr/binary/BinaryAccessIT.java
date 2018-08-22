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
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.getBinary;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.httpGet;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.httpPut;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.isFailedHttpPut;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.isSuccessfulHttpPut;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.putBinary;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.storeBinary;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.storeBinaryAndRetrieve;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.util.Content;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Integration test for direct binary GET/PUT via HTTP, that requires a fully working data store
 * (such as S3) for each {@link AbstractBinaryAccessIT#dataStoreFixtures() configured fixture}.
 * The data store in question must support direct GET/PUT access via a URI.
 * 
 * Data store must be configured through e.g. aws.properties.
 *
 * Run this IT in maven using either:
 *
 *   single test:
 *     mvn clean test -Dtest=BinaryAccessIT
 * 
 *   as part of all integration tests:
 *     mvn -PintegrationTesting clean install
 */
@RunWith(Parameterized.class)
public class BinaryAccessIT extends AbstractBinaryAccessIT {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String FILE_PATH = "/file";
    private static final int REGULAR_WRITE_EXPIRY = 60*5; // seconds
    private static final int REGULAR_READ_EXPIRY = 60*5; // seconds

    public BinaryAccessIT(NodeStoreFixture fixture) {
        // reuse NodeStore (and DataStore) across all tests in this class
        super(fixture, true);
    }

    private JackrabbitValueFactory uploadProvider;
    private JackrabbitValueFactory anonymousUploadProvider;

    @Before
    public void cleanRepoContents() throws RepositoryException {
        Session anonymousSession = getAnonymousSession();
        uploadProvider = (JackrabbitValueFactory) getAdminSession().getValueFactory();
        anonymousUploadProvider = (JackrabbitValueFactory) anonymousSession.getValueFactory();

        if (getAdminSession().nodeExists(FILE_PATH)) {
            getAdminSession().getNode(FILE_PATH).remove();
            getAdminSession().save();
        }
    }

    // F1 - basic test
    @Test
    public void testUpload() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        Content content = Content.createRandom(256);

        assertTrue(getAdminSession().getValueFactory() instanceof JackrabbitValueFactory);

        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 1);
        assertNotNull(upload);

        // very small test binary
        assertTrue(content.size() < upload.getMaxPartSize());

        URI uri = upload.getUploadURIs().iterator().next();
        assertNotNull(uri);

        log.info("- uploading binary via PUT to {}", uri.toString());
        int code = httpPut(uri, content.size(), content.getStream());

        assertTrue("PUT to pre-signed URI failed",
                isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary writeBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        putBinary(getAdminSession(), FILE_PATH, writeBinary);

        Binary readBinary = getBinary(getAdminSession(), FILE_PATH);
        content.assertEqualsWith(readBinary.getStream());
    }

    // F3 - Multi-part upload
    @Test
    public void testMultiPartUpload() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        assertTrue(getAdminSession().getValueFactory() instanceof JackrabbitValueFactory);

        // 25MB is a good size to ensure chunking is done
        Content content = Content.createRandom(1024 * 1024 * 25);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 50);
        assertNotNull(upload);

        List<URI> uris = new ArrayList<>();
        Iterables.addAll(uris, upload.getUploadURIs());

        // this follows the upload algorithm from BinaryUpload
        if (content.size() / upload.getMaxPartSize() > uris.size()) {
            fail("exact binary size was provided but implementation failed to provide enough upload URIs");
        }
        if (content.size() < upload.getMinPartSize()) {
            // single upload
            content.httpPUT(uris.get(0));
        } else {
            // multipart upload
            final long basePartSize = (long) Math.ceil(content.size() / (double) uris.size());
            
            long offset = 0;
            for (URI uri : uris) {
                final long partSize = Math.min(basePartSize, content.size() - offset);

                int code = content.httpPUT(uri, offset, partSize);
                assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

                offset += partSize;
                // fail safe check, shouldn't be necessary
                if (offset >= content.size()) {
                    break;
                }
            }
        }

        Binary writeBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        putBinary(getAdminSession(), FILE_PATH, writeBinary);

        Binary readBinary = getBinary(getAdminSession(), FILE_PATH);
        content.assertEqualsWith(readBinary.getStream());
    }

    // F8 - test reading getBinary().toInputStream() once uploaded
    @Test
    public void testStreamBinaryThroughJCRAfterURIWrite() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        // 1. add binary and upload
        Content content = Content.createRandom(256);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 10);
        int code = content.httpPUT(upload.getUploadURIs().iterator().next());
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary binaryWrite = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        storeBinary(getAdminSession(), FILE_PATH, binaryWrite);

        // 2. stream through JCR and validate it's the same
        Session session = createAdminSession();
        try {
            Binary binaryRead = getBinary(session, FILE_PATH);
            content.assertEqualsWith(binaryRead.getStream());
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
        Content content = Content.createRandom(1024*20);

        // make sure to test getting a fresh Binary
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);
        assertTrue(binary instanceof BinaryDownload);

        URI downloadURI = ((BinaryDownload) binary).getURI(BinaryDownloadOptions.DEFAULT);
        assertNotNull("HTTP download URI is null", downloadURI);
        content.assertEqualsWith(httpGet(downloadURI));

        // different way to retrieve binary
        // TODO: also test multivalue binary prop
        binary = getAdminSession().getNode(FILE_PATH)
            .getNode(JcrConstants.JCR_CONTENT)
            .getProperty(JcrConstants.JCR_DATA).getValue().getBinary();
        downloadURI = ((BinaryDownload) binary).getURI(BinaryDownloadOptions.DEFAULT);
        assertNotNull("HTTP download URI is null", downloadURI);
    }

    // F9 - GET Binary for binary after write using direct PUT
    @Test
    public void testGetBinaryAfterPut() throws Exception {
        // enable writable and readable URI feature
        ConfigurableDataRecordAccessProvider provider = getConfigurableHttpDataRecordProvider();
        provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        // 1. add binary and upload
        Content content = Content.createRandom(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 10);
        int code = content.httpPUT(upload.getUploadURIs().iterator().next());
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary writeBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        storeBinary(getAdminSession(), FILE_PATH, writeBinary);

        // 2. read binary, get the URI
        Binary binary = getBinary(getAdminSession(), FILE_PATH);
        URI downloadURI = ((BinaryDownload)(binary)).getURI(BinaryDownloadOptions.DEFAULT);

        // 3. GET on URI and verify contents are the same
        content.assertEqualsWith(httpGet(downloadURI));
    }

    @Test
    public void testGetSmallBinaryReturnsNull() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        // Must be smaller than the minimum file size, (inlined binary)
        Content content = Content.createRandom(256);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        URI downloadURI = ((BinaryDownload)(binary)).getURI(BinaryDownloadOptions.DEFAULT);
        assertNull(downloadURI);
    }

    @Test
    public void testGetBinaryWithSpecificMediaType() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        String expectedMediaType = "image/png";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withMediaType(expectedMediaType)
                .build();
        URI downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        assertNotNull(mediaType);
        assertEquals(expectedMediaType, mediaType);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        content.assertEqualsWith(conn.getInputStream());
    }

    @Test
    public void testGetBinaryWithSpecificMediaTypeAndEncoding() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        String expectedMediaType = "text/plain";
        String expectedCharacterEncoding = "utf-8";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withMediaType(expectedMediaType)
                .withCharacterEncoding(expectedCharacterEncoding)
                .build();
        URI downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        assertNotNull(mediaType);
        assertEquals(String.format("%s; charset=%s", expectedMediaType, expectedCharacterEncoding),
                mediaType);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        content.assertEqualsWith(conn.getInputStream());
    }

    @Test
    public void testGetBinaryWithCharacterEncodingOnly() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        String expectedCharacterEncoding = "utf-8";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withCharacterEncoding(expectedCharacterEncoding)
                .build();
        URI downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String mediaType = conn.getHeaderField("Content-Type");
        // application/octet-stream is the default Content-Type if none is
        // set in the signed URI
        assertEquals("application/octet-stream", mediaType);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        content.assertEqualsWith(conn.getInputStream());
    }

    @Test
    public void testGetBinaryWithSpecificFileName() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        String expectedName = "beautiful landscape.png";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withFileName(expectedName)
                .build();
        URI downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

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
        content.assertEqualsWith(conn.getInputStream());
    }

    @Test
    public void testGetBinaryWithSpecificFileNameAndDispositionType() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        String expectedName = "beautiful landscape.png";
        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withFileName(expectedName)
                .withDispositionTypeAttachment()
                .build();
        URI downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

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
        content.assertEqualsWith(conn.getInputStream());
    }

    @Test
    public void testGetBinaryWithDispositionType() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        BinaryDownloadOptions downloadOptions = BinaryDownloadOptions
                .builder()
                .withDispositionTypeInline()
                .build();
        URI downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        String contentDisposition = conn.getHeaderField("Content-Disposition");
        // Should be no header since filename was not set and disposition type
        // is "inline"
        assertNull(contentDisposition);

        // Verify response content
        assertEquals(200, conn.getResponseCode());
        content.assertEqualsWith(conn.getInputStream());

        downloadOptions = BinaryDownloadOptions
                .builder()
                .withDispositionTypeAttachment()
                .build();
        downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

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

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

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
        URI downloadURI = ((BinaryDownload)(binary)).getURI(downloadOptions);

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
        content.assertEqualsWith(conn.getInputStream());
    }

    @Test
    public void testGetBinaryDefaultOptions() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        URI downloadURI = ((BinaryDownload)(binary)).getURI(BinaryDownloadOptions.DEFAULT);

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
        content.assertEqualsWith(conn.getInputStream());
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

        Content content = Content.createRandom(256);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 10);

        assertNotNull(upload);
        assertFalse(upload.getUploadURIs().iterator().hasNext());
    }

    // A2 - disable get URIs entirely
    @Test
    public void testDisableDirectHttpDownload() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectDownloadURIExpirySeconds(0);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        URI downloadURI = ((BinaryDownload)(binary)).getURI(BinaryDownloadOptions.DEFAULT);
        assertNull(downloadURI);
    }

    // A2/A3 - configure short expiry time, wait, ensure upload fails after expired
    @Test
    public void testPutURIExpires() throws Exception {
        // short timeout
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(1);

        Content content = Content.createRandom(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 10);

        // wait to pass timeout: 2 seconds
        Thread.sleep(2 * 1000);

        // ensure PUT fails with 403 or anything 400+
        assertTrue(content.httpPUT(upload.getUploadURIs().iterator().next()) >= HttpURLConnection.HTTP_BAD_REQUEST);
    }

    // F2 - transfer accelerator (S3 only feature)
    @Test
    public void testTransferAcceleration() throws Exception {
        ConfigurableDataRecordAccessProvider provider = getConfigurableHttpDataRecordProvider();

        // This test is S3 specific
        if (provider instanceof S3DataStore) {

            provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
            provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);
            provider.setBinaryTransferAccelerationEnabled(true);

            BinaryUpload upload = uploadProvider.initiateBinaryUpload(1024 * 20, 1);
            URI uri = upload.getUploadURIs().iterator().next();
            assertNotNull(uri);

            log.info("accelerated URI: {}", uri.toString());
            assertTrue(uri.getHost().endsWith(".s3-accelerate.amazonaws.com"));

            provider.setBinaryTransferAccelerationEnabled(false);
            upload = uploadProvider.initiateBinaryUpload(1024*20, 1);
            uri = upload.getUploadURIs().iterator().next();
            assertNotNull(uri);

            log.info("non-accelerated URI: {}", uri.toString());
            assertFalse(uri.getHost().endsWith(".s3-accelerate.amazonaws.com"));
        }
    }

    // A1 - get put URI, change it and try uploading it
    @Test
    public void testModifiedPutURIFails() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        Content content = Content.createRandom(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 1);
        URI uri = upload.getUploadURIs().iterator().next();
        URI changedURI = new URI(
                String.format("%s://%s/%sX?%s",  // NOTE the injected "X" in the URI filename
                uri.getScheme(),
                uri.getHost(),
                uri.getPath(),
                uri.getQuery())
        );
        int code = content.httpPUT(changedURI);
        assertTrue(isFailedHttpPut(code));
    }

    // A1 - get put URI, upload, then try reading from the same URI
    @Test
    public void testCannotReadFromPutURI() throws Exception {
        // enable writable URI and readable URI feature
        ConfigurableDataRecordAccessProvider provider = getConfigurableHttpDataRecordProvider();
        provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 1);
        URI uri = upload.getUploadURIs().iterator().next();
        int code = content.httpPUT(uri);
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
        code = conn.getResponseCode();
        assertTrue(isFailedHttpPut(code));
    }

    // A1 - add binary via JCR, then get put URI, and modify to try to upload over first binary
    @Test
    public void testCannotModifyExistingBinaryViaPutURI() throws Exception {
        // enable writable URI and readable URI feature
        ConfigurableDataRecordAccessProvider provider = getConfigurableHttpDataRecordProvider();
        provider.setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);
        provider.setDirectDownloadURIExpirySeconds(REGULAR_READ_EXPIRY);

        Content content = Content.createRandom(1024*20);
        Binary binary = storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        URI downloadURI = ((BinaryDownload)(binary)).getURI(BinaryDownloadOptions.DEFAULT);
        assertNotNull(downloadURI);

        Content moreContent = Content.createRandom(1024*20);
        uploadProvider.initiateBinaryUpload(moreContent.size(), 1);
        HttpURLConnection conn = (HttpURLConnection) downloadURI.toURL().openConnection();
        conn.setRequestMethod("PUT");
        conn.setDoOutput(true);
        IOUtils.copy(moreContent.getStream(), conn.getOutputStream());

        int code = conn.getResponseCode();
        assertTrue(isFailedHttpPut(code));

        content.assertEqualsWith(httpGet(downloadURI));
    }

    // D1 - immutable after initial upload
    @Test
    public void testUploadedBinaryIsImmutable() throws Exception {
        // enable writable URI feature
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        // 1. upload and store first binary
        Content content = Content.createRandom(1024*20);
        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 1);
        assertNotNull(upload);
        int code = content.httpPUT(upload.getUploadURIs().iterator().next());
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary uploadedBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        storeBinary(getAdminSession(), FILE_PATH, uploadedBinary);

        Binary binary1 = getBinary(getAdminSession(), FILE_PATH);

        // 2. upload different binary content, but store at the same JCR location
        Content moreContent = Content.createRandom(1024*21);
        upload = uploadProvider.initiateBinaryUpload(moreContent.size(), 1);
        assertNotNull(upload);
        code = moreContent.httpPUT(upload.getUploadURIs().iterator().next());
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        uploadedBinary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        storeBinary(getAdminSession(), FILE_PATH, uploadedBinary);

        Binary binary2 = getBinary(getAdminSession(), FILE_PATH);

        // 3. verify they have different references
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

        Content content = Content.createRandom(1024*20);

        BinaryUpload upload1 = uploadProvider.initiateBinaryUpload(content.size(), 1);
        assertNotNull(upload1);
        BinaryUpload upload2 = uploadProvider.initiateBinaryUpload(content.size(), 1);
        assertNotNull(upload2);

        assertNotEquals(upload1.getUploadURIs().iterator().next().toString(),
                upload2.getUploadURIs().iterator().next().toString());
    }

    // D3 - do not delete directly => copy nt:file node, delete one, ensure binary still there
    @Test
    public void testBinaryNotDeletedWithNode() throws Exception {
        getConfigurableHttpDataRecordProvider()
                .setDirectUploadURIExpirySeconds(REGULAR_WRITE_EXPIRY);

        Content content = Content.createRandom(1024*20);

        BinaryUpload upload = uploadProvider.initiateBinaryUpload(content.size(), 1);
        int code = content.httpPUT(upload.getUploadURIs().iterator().next());
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));
        Binary binary = uploadProvider.completeBinaryUpload(upload.getUploadToken());
        storeBinary(getAdminSession(), FILE_PATH+"2", binary);

        storeBinary(getAdminSession(), FILE_PATH, binary);

        getAdminSession().getNode(FILE_PATH+"2").remove();
        getAdminSession().save();

        Binary savedBinary = getBinary(getAdminSession(), FILE_PATH);
        assertNotNull(savedBinary);
        assertTrue(binary instanceof ReferenceBinary);
        assertTrue(savedBinary instanceof ReferenceBinary);
        assertEquals(((ReferenceBinary) binary).getReference(),
                ((ReferenceBinary) savedBinary).getReference());
    }

    // D5 - blob ref not persisted in NodeStore until binary uploaded and immutable
    // NOTE: not test needed for this, as the API guarantees the Binary is only returned after
    //       the blob was persisted in completeBinaryUpload() and client code is responsible
    //       for writing it to the JCR

    // more tests

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
