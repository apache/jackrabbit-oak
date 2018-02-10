/**************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * __________________
 *
 *  Copyright 2018 Adobe Systems Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 *************************************************************************/

package org.apache.jackrabbit.oak.jcr.binary;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.oak.api.binary.URLWritableBinary;
import org.apache.jackrabbit.oak.api.binary.URLWritableBinaryValueFactory;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration test for URLWritableBinary and URLReadableBinary, that requires a fully working data store
 * (such as S3) for each {@link AbstractURLBinaryIT#dataStoreFixtures() configured fixture}.
 * Data store must be configured through s3.properties.
 */
public class URLBinaryIT extends AbstractURLBinaryIT {

    private static final String FILE_PATH = "/file";

    public URLBinaryIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    // TODO: one test for each requirement
    // F2 - CDN & transfer accelerators
    // F3 - chunked upload
    // F4 - S3 and Azure => through parametrization using S3 and Azure fixtures
    // F5 - more cloud stores => new fixtures, mock fixture
    // F6/F7 - no additional final request, notification API via SQS
    // F9 - URLReadableBinary for binary after read
    // F10 - URLReadableBinary for binary added through InputStream (has to wait for S3 upload)

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
        Node file = getOrCreateNtFile(getAdminSession(), FILE_PATH);

        ValueFactory valueFactory = getAdminSession().getValueFactory();
        assertTrue(valueFactory instanceof URLWritableBinaryValueFactory);

        URLWritableBinary binary = ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
        assertNotNull(binary);

        // ensure not accessible before setting a property
        assertNull(binary.getWriteURL());

        file.setProperty(JcrConstants.JCR_DATA, binary);

        // ensure not accessible before successful save
        assertNull(binary.getWriteURL());

        getAdminSession().save();

        URL url = binary.getWriteURL();
        assertNotNull(url);

        System.out.println("- uploading binary via PUT to " + url);
        int code = httpPutTestStream(url);

        Assert.assertEquals("PUT to pre-signed S3 URL failed", 200, code);

        Binary binary2 = getBinary(getAdminSession(), FILE_PATH);
        binary2.getStream();
    }

    // F8 - test reading getBinary().getInputStream() once uploaded
    @Test
    public void testStreamBinaryThroughJCRAfterURLWrite() throws Exception {
        final String CONTENT = "hi there, I'm a binary blob!";

        // 1. add binary and upload
        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
        httpPut(binary.getWriteURL(), getTestInputStream(CONTENT));

        Thread.sleep(1 * SECONDS);

        // 2. stream through JCR and validate it's the same
        Binary binaryRead = getBinary(createAdminSession(), FILE_PATH);
        assertTrue(IOUtils.contentEquals(binaryRead.getStream(), getTestInputStream(CONTENT)));
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
        getDataStore().setURLWritableBinaryExpirySeconds(0);

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
        getDataStore().setURLWritableBinaryExpirySeconds(1);

        URLWritableBinary binary = saveFileWithURLWritableBinary(getAdminSession(), FILE_PATH);
        URL url = binary.getWriteURL();

        // wait to pass timeout
        Thread.sleep(2 * SECONDS);

        // ensure PUT fails with 403 or anything 400+
        assertTrue(httpPutTestStream(url) > HttpURLConnection.HTTP_BAD_REQUEST);
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
    private URLWritableBinary saveFileWithURLWritableBinary(Session session, String path) throws RepositoryException {
        Node resource = getOrCreateNtFile(session, path);

        ValueFactory valueFactory = session.getValueFactory();
        assertTrue(valueFactory instanceof URLWritableBinaryValueFactory);

        URLWritableBinary binary = ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
        assertNotNull("URLWritableBinary not supported", binary);
        resource.setProperty(JcrConstants.JCR_DATA, binary);
        session.save();

        return binary;
    }
}
