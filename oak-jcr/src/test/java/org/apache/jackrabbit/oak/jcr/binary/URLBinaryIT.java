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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

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

    public URLBinaryIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    // TODO: one test for each requirement
    // F1 - basic test
    // F2 - CDN & transfer accelerators
    // F3 - chunked upload
    // F4 - S3 and Azure => through parametrization using S3 and Azure fixtures
    // F5 - more cloud stores => new fixtures, mock fixture
    // F6/F7 - no additional final request, notification API via SQS

    // A1 - get put url, change it and try uploading somewhere else in S3
    // A2 - configure short expiry time, wait, ensure upload fails after expired
    // A3 - covered by A2
    // A4 - no test, SHOULD requirement only, hard to test
    // A5 - get S3 URL (how?) and try an upload
    // A7 - only get write access after all AC checks/session.save() => like A6 but test before save

    // D1 - immutable after initial upload
    // D2 - unique identifiers
    // D3 - do not delete directly => copy nt:file node, delete one, ensure binary still there
    // D4 - same as A7
    // D5 - support dangling ref => get binary before upload, catch expected exception etc.
    // DX - get existing regular binary and try to overwrite it (doesn't work)

    // A6 - Client MUST only get permission to add a blob referenced in a JCR binary property
    //      where the user has JCR set_property permission.
    @Test
    public void testWritePermissionRequired() throws Exception {
        // 1. create URL access binary
        addNtFileWithURLWritableBinary(getAdminSession(), "/file");

        // 2. then get existing url access binary using read-only session
        URLWritableBinary urlWritableBinary = (URLWritableBinary) getBinary(createAnonymousSession(), "/file");
        try {
            // 3. ensure trying to get writeable URL fails
            urlWritableBinary.getPutURL();
            fail("did not throw AccessDeniedException when session does not have write permissions on the property");
        } catch (AccessDeniedException ignored) {
        }
    }

    @Test
    public void testURLWritableBinary() throws Exception {
        // 1. check if url access binary is supported? no => 2, yes => 3
        // 2. no support: create structure with no/empty binary prop, overwrite later in 2nd request with InputStream
        // 3. state intention for an URLWritableBinary, so oak knows it needs to generate a unique UUID and no content hash
        // 4. save() session (acl checks only happen fully upon save())
        // 5. retrieve URLWritableBinary again, now put-enabled due to ACL checks in 4.
        // 6. get Put URL from URLWritableBinary

        Session session = createAdminSession();
        Node file = getOrCreateNtFile(session, "/file");

        ValueFactory valueFactory = session.getValueFactory();

        Binary placeholderBinary = null;
        if (valueFactory instanceof URLWritableBinaryValueFactory) {
            System.out.println(">>> YES url binary support [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅] [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅] [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅] [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅]");
            // might return null if url access binaries are not configured
            placeholderBinary = ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
        }
        if (placeholderBinary == null) {
            // fallback
            System.out.println(">>> NO url binary support");
            // TODO: normally, a client would set an empty binary here and overwrite with an inputstream in a future, 2nd request
            // generate 2 MB of meaningless bytes
            placeholderBinary = valueFactory.createBinary(getTestInputStream(2 * MB));
        }
        Value binaryValue = valueFactory.createValue(placeholderBinary);
        file.setProperty(JcrConstants.JCR_DATA, binaryValue);
        session.save();

        // have to retrieve the persisted binary again to get access to the the URL
        Binary binary = getBinary(session, "/file");
        if (binary instanceof URLWritableBinary) {
            URLWritableBinary urlWritableBinary = (URLWritableBinary) binary;
            String putURL = urlWritableBinary.getPutURL();
            assertNotNull(putURL);
            System.out.println("- uploading binary via PUT to " + putURL);
            int code = httpPut(new URL(putURL), getTestInputStream("hello world"));
            Assert.assertEquals("PUT to pre-signed S3 URL failed", 200, code);
        }
    }

    @Test
    public void testDisabledURLWritableBinary() throws Exception {
        // disable in datastore config by setting expiry to zero
        getDataStore().setURLWritableBinaryExpiryTime(0);

        URLWritableBinary binary = addNtFileWithURLWritableBinary(getAdminSession(), "/file");
        // TODO: we might want to not return a URLWritableBinary in the first place if it's disabled
        assertNotNull(binary);
        assertNull(binary.getPutURL());
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

    private Binary createURLWritableBinary() throws RepositoryException {
        Session session = getAdminSession();

        ValueFactory valueFactory = session.getValueFactory();
        if (valueFactory instanceof URLWritableBinaryValueFactory) {
            return ((URLWritableBinaryValueFactory) valueFactory).createURLWritableBinary();
        }
        return null;
    }

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
    private URLWritableBinary addNtFileWithURLWritableBinary(Session session, String path) throws RepositoryException {
        Node resource = getOrCreateNtFile(session, path);
        Binary binary = createURLWritableBinary();
        resource.setProperty(JcrConstants.JCR_DATA, binary);
        session.save();

        Binary binary2 = resource.getProperty(JcrConstants.JCR_DATA).getBinary();
        if (binary2 instanceof URLWritableBinary) {
            return (URLWritableBinary) binary2;
        }
        return null;
    }

    private static InputStream getTestInputStream(String content) {
        try {
            return new ByteArrayInputStream(content.getBytes("utf-8"));
        } catch (UnsupportedEncodingException unexpected) {
            unexpected.printStackTrace();
            // return empty stream
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    private static InputStream getTestInputStream(int size) {
        byte[] blob = new byte[size];
        // magic bytes so it's not just all zeros
        blob[0] = 1;
        blob[1] = 2;
        blob[2] = 3;
        return new ByteArrayInputStream(blob);
    }
}
