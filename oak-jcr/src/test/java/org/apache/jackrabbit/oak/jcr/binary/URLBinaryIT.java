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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.oak.api.binary.URLReadableBinary;
import org.apache.jackrabbit.oak.api.binary.URLWritableBinary;
import org.apache.jackrabbit.oak.api.binary.URLWritableBinaryValueFactory;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Integration test for URLWritableBinary and URLReadableBinary, that requires a fully working data store
 * (such as S3) for each {@link AbstractURLBinaryIT#dataStoreFixtures() configured fixture}.
 * 
 * Data store must be configured through aws.properties.
 *
 * Run this IT in maven using either:
 *
 *   single test:
 *     mvn clean test -Dtest=URLBinaryIT
 * 
 *   as part of all integration tests:
 *     mvn -PintegrationTesting clean install
 */
@RunWith(Parameterized.class)
public class URLBinaryIT extends AbstractURLBinaryIT {

    private static final String FILE_PATH = "/file";
    private static final String CONTENT = "hi there, I'm a binary blob!";
    private static final int REGULAR_WRITE_EXPIRY = 60; // seconds
    private static final int REGULAR_READ_EXPIRY = 60; // seconds

    public URLBinaryIT(NodeStoreFixture fixture) {
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
    // F3 - chunked upload
    // F4 - S3 and Azure => through parametrization using S3 and Azure fixtures
    // F5 - more cloud stores => new fixtures, mock fixture
    // F6/F7 - no additional final request, notification API via SQS

    // A1 - get put url, change it and try uploading somewhere else in S3
    // A4 - no test, SHOULD requirement only, hard to test
    // A5 - get S3 URL (how?) and try an upload
    // A7 - only get write access after all AC checks/session.save() => like A6 but test before save

    // D1 - immutable after initial upload
    // D2 - unique identifiers
    // D3 - do not delete directly => copy nt:file node, delete one, ensure binary still there
    // D4 - same as A7
    // D5 - support dangling ref => get binary before upload, catch expected exception etc.
    // DX - get existing regular binary and try to overwrite it (doesn't work)

    // F1 - basic test
    @Test
    public void testURLWritableBinary() throws Exception {
        // enable writable URL feature
        getURLWritableDataStore().setURLWritableBinaryExpirySeconds(REGULAR_WRITE_EXPIRY);

        // create JCR nt:file structure for holding binary
        Node file = getOrCreateNtFile(getAdminSession(), FILE_PATH);

        ValueFactory valueFactory = getAdminSession().getValueFactory();
        assertTrue(valueFactory instanceof URLWritableBinaryValueFactory);

        // create new binary
        URLWritableBinary binary = ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
        assertNotNull(binary);

        // ensure not accessible before setting a property
        assertNull(binary.getWriteURL());

        file.setProperty(JcrConstants.JCR_DATA, binary);

        // ensure not accessible before successful save
        assertNull(binary.getWriteURL());

        getAdminSession().save();

        // validate
        URL url = binary.getWriteURL();
        assertNotNull(url);

        System.out.println("- uploading binary via PUT to " + url);
        int code = httpPutTestStream(url);

        assertEquals("PUT to pre-signed S3 URL failed", 200, code);

        Binary binary2 = getBinary(getAdminSession(), FILE_PATH);
        binary2.getStream();
    }

    // D1/S2 - test reading getBinary().getInputStream() once uploaded
    @Test
    public void testWriteURLDoesNotChange() throws Exception {
        // enable writable URL feature
        getURLWritableDataStore().setURLWritableBinaryExpirySeconds(REGULAR_WRITE_EXPIRY);

        // 1. add binary
        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);

        // 2. request url for the 1st time (and check it's not null)
        URL url = binary.getWriteURL();
        assertNotNull(url);

        // 3. assert that 2nd request yields the exact same URL
        assertEquals(url, binary.getWriteURL());
    }

    // F8 - test reading getBinary().getInputStream() once uploaded
    @Test
    public void testStreamBinaryThroughJCRAfterURLWrite() throws Exception {
        // enable writable URL feature
        getURLWritableDataStore().setURLWritableBinaryExpirySeconds(REGULAR_WRITE_EXPIRY);

        // 1. add binary and upload
        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
        httpPut(binary.getWriteURL(), getTestInputStream(CONTENT));

        // 2. stream through JCR and validate it's the same
        Binary binaryRead = getBinary(createAdminSession(), FILE_PATH);
        assertTrue(IOUtils.contentEquals(binaryRead.getStream(), getTestInputStream(CONTENT)));
    }

    // F9 - URLReadableBinary for binary after write using URLWritableBinary
    @Test
    public void testURLReadableBinary() throws Exception {
        // enable writable and readable URL feature
        getURLWritableDataStore().setURLWritableBinaryExpirySeconds(REGULAR_WRITE_EXPIRY);
        getURLReadableDataStore().setURLReadableBinaryExpirySeconds(REGULAR_READ_EXPIRY);

        // 1. add binary and upload
        URLWritableBinary newBinary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
        httpPut(newBinary.getWriteURL(), getTestInputStream(CONTENT));

        // 2. read binary, check it's a URLReadableBinary and get the URL
        Binary binary = getBinary(getAdminSession(), FILE_PATH);
        assertTrue(binary instanceof URLReadableBinary);
        URL url = ((URLReadableBinary) binary).getReadURL();
        assertNotNull(url);

        // 3. GET on URL and verify contents are the same
        InputStream stream = httpGet(url);
        assertTrue(IOUtils.contentEquals(stream, getTestInputStream(CONTENT)));
    }

    // F10 - URLReadableBinary for binary added through InputStream (has to wait for S3 upload)
    @Test
    public void testURLReadableBinaryFromInputStream() throws Exception {
        // enable readable URL feature
        getURLReadableDataStore().setURLReadableBinaryExpirySeconds(REGULAR_READ_EXPIRY);

        // 1. add binary via input stream (must be larger than 16 KB segmentstore inline binary limit)
        Node resource = getOrCreateNtFile(getAdminSession(), FILE_PATH);
        Binary streamBinary = getAdminSession().getValueFactory().createBinary(getTestInputStream(MB));
        resource.setProperty(JcrConstants.JCR_DATA, streamBinary);

        waitForUploads();

        // 2. read binary, check it's a URLReadableBinary and get the URL
        Binary binary = getBinary(getAdminSession(), FILE_PATH);
        assertTrue(binary instanceof URLReadableBinary);
        URL url = ((URLReadableBinary) binary).getReadURL();
        assertNotNull(url);

        // 3. GET on URL and verify contents are the same
        InputStream stream = httpGet(url);
        assertTrue(IOUtils.contentEquals(stream, getTestInputStream(MB)));
    }

    // A6 - Client MUST only get permission to add a blob referenced in a JCR binary property
    //      where the user has JCR set_property permission.
    @Test
    public void testReadingBinaryDoesNotReturnURLWritableBinary() throws Exception {
        // 1. create URL access binary
        saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);

        // 2. then get existing url access binary using read-only session
        Binary binary = getBinary(createAnonymousSession(), FILE_PATH);

        // 3. ensure we do not get a writable binary
        assertFalse(binary instanceof URLWritableBinary);
    }

    // A2 - disable write URLs entirely
    @Test
    public void testDisabledURLWritableBinary() throws Exception {
        // disable in data store config by setting expiry to zero
        getURLWritableDataStore().setURLWritableBinaryExpirySeconds(0);

        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
        // TODO: we might want to not return a URLWritableBinary in the first place if it's disabled
        assertNotNull(binary);
        assertNull(binary.getWriteURL());

        // TODO: extra test, showing alternative input stream code working if disabled
/*
        if (placeholderBinary == null) {
            // fallback
            System.out.println(">>> NO url binary support");
            // TODO: normally, a client would set an empty binary here and overwrite with an inputstream in a future, 2nd request
            // generate 2 MB of meaningless bytes
            placeholderBinary = valueFactory.createBinary(getTestInputStream(2 * MB));
        }
*/
    }

    // A2/A3 - configure short expiry time, wait, ensure upload fails after expired
    @Test
    public void testExpiryOfURLWritableBinary() throws Exception {
        // short timeout
        getURLWritableDataStore().setURLWritableBinaryExpirySeconds(1);

        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
        URL url = binary.getWriteURL();

        // wait to pass timeout
        Thread.sleep(2 * SECONDS);

        // ensure PUT fails with 403 or anything 400+
        assertTrue(httpPutTestStream(url) > HttpURLConnection.HTTP_BAD_REQUEST);
    }

    // this tests an initial unexpected side effect bug where the underlying URLWritableBlob implementation
    // failed to work in a hash set with at least two blobs because of default equals/hashCode methods trying
    // to read the length() of the binary which is not available for to be written URLWritableBlobs
    @Test
    public void testMultipleURLWritableBinariesInOneSave() throws Exception {
        getURLWritableDataStore().setURLWritableBinaryExpirySeconds(REGULAR_WRITE_EXPIRY);

        URLWritableBinary binary1 = createFileWithURLWritableBinary(getAdminSession(), FILE_PATH, false);
        URLWritableBinary binary2 = createFileWithURLWritableBinary(getAdminSession(), FILE_PATH + "2", false);
        getAdminSession().save();

        URL url1 = binary1.getWriteURL();
        assertNotNull(url1);
        assertEquals(200, httpPutTestStream(url1));
        URL url2 = binary2.getWriteURL();
        assertNotNull(url2);
        assertEquals(200, httpPutTestStream(url2));
    }

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

    private Binary getBinary(Session session, String ntFilePath) throws RepositoryException {
        return session.getNode(ntFilePath)
            .getNode(JcrConstants.JCR_CONTENT)
            .getProperty(JcrConstants.JCR_DATA)
            .getBinary();
    }

    /** Creates an nt:file with an url access binary at the given path and saves the session. */
    @Nonnull
    private URLWritableBinary saveFileWithURLWritableBinary(Session session, String path) throws RepositoryException {
        return createFileWithURLWritableBinary(session, path, true);
    }

    /** Creates an nt:file with an url access binary at the given path. */
    @Nonnull
    private URLWritableBinary createFileWithURLWritableBinary(Session session, String path, boolean autoSave) throws RepositoryException {
        Node resource = getOrCreateNtFile(session, path);

        ValueFactory valueFactory = session.getValueFactory();
        assertTrue(valueFactory instanceof URLWritableBinaryValueFactory);

        URLWritableBinary binary = ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
        assertNotNull("URLWritableBinary not supported", binary);
        resource.setProperty(JcrConstants.JCR_DATA, binary);

        if (autoSave) {
            session.save();
        }

        return binary;
    }

}
