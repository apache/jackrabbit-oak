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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import javax.annotation.CheckForNull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Binary;
import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.Privilege;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.binary.ExternalBinary;
import org.apache.jackrabbit.oak.api.binary.ExternalBinaryValueFactory;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExternalBinaryIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target")) {
        @Override
        protected void after() {
            //delete();
        }
    };
    
    private File dataStoreDir;
    private Properties s3Props;

    @Before
    public void setUp() throws Exception {
        dataStoreDir = folder.newFolder();
        s3Props = new Properties();
        s3Props.load(new FileReader(System.getProperty("s3.config", "s3.properties")));
    }

    protected NodeStore buildNodeStoreWithS3() throws IOException {
        return SegmentNodeStoreBuilders.builder(new MemoryStore() {
            @CheckForNull
            @Override
            public BlobStore getBlobStore() {
                try {
                    S3DataStore s3 = new S3DataStore();
                    s3.setProperties(s3Props);
                    s3.init(dataStoreDir.getAbsolutePath());
                    return new DataStoreBlobStore(s3);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).build();
    }

    protected Session createAdminSession(Repository repository) throws RepositoryException {
        return repository.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID.toCharArray()));
    }

    protected Session createAnonymousSession(Repository repository) throws RepositoryException {
        Session admin = createAdminSession(repository);
        AccessControlUtils.addAccessControlEntry(admin, "/", EveryonePrincipal.getInstance(), new String[] {Privilege.JCR_READ}, true);
        admin.save();
        return repository.login(new GuestCredentials());
    }

    @Test
    public void testExternalBinary() throws Exception {
        Repository repository = new Jcr(
            new Oak(
                buildNodeStoreWithS3()
            )
        ).createRepository();
        Session session = createAdminSession(repository);
        Node file = session.getRootNode().addNode("file");

        ValueFactory valueFactory = session.getValueFactory();

        // 1. check if external binary is supported? no => 2, yes => 3
        // 2. no support: create structure with no/empty binary prop, overwrite later in 2nd request with InputStream
        // 3. state intention for an ExternalBinary, so oak knows it needs to generate a unique UUID and no content hash
        // 4. save() session (acl checks only happen fully upon save())
        // 5. retrieve ExternalBinary again, now put-enabled due to ACL checks in 4.
        // 6. get Put URL from ExternalBinary

        Binary placeholderBinary = null;
        if (valueFactory instanceof ExternalBinaryValueFactory) {
            System.out.println(">>> YES external binary support [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅] [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅] [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅] [̲̅$̲̅(̲̅1̲̅)̲̅$̲̅]");
            // might return null if external binaries are not configured
            placeholderBinary = ((ExternalBinaryValueFactory) valueFactory).createNewExternalBinary();
        }
        if (placeholderBinary == null) {
            // fallback
            System.out.println(">>> NO external binary support");
            // TODO: normally, a client would set an empty binary here and overwrite with an inputstream in a future, 2nd request
            // generate 2 MB of meaning less bytes
            placeholderBinary = valueFactory.createBinary(getTestInputStream(2 * 1024 * 1024));
        }
        Value binaryValue = valueFactory.createValue(placeholderBinary);
        file.setProperty("binary", binaryValue);
        session.save();

        // have to retrieve the persisted binary again to get access to the the URL
        Binary binary = file.getProperty("binary").getBinary();
        if (binary instanceof ExternalBinary) {
            ExternalBinary externalBinary = (ExternalBinary) binary;
            String putURL = externalBinary.getPutURL();
            System.out.println("- uploading binary via PUT to " + putURL);
            int code = httpPut(new URL(putURL), getTestInputStream("hello world"));
            Assert.assertEquals("PUT to pre-signed S3 URL failed", 200, code);
        }

        Session anonymousSession = createAnonymousSession(repository);
        Binary anonBinary = anonymousSession.getNode("/file").getProperty("binary").getBinary();
        if (anonBinary instanceof ExternalBinary) {
            ExternalBinary extAnonBinary = (ExternalBinary) anonBinary;
            try {
                extAnonBinary.getPutURL();
                Assert.fail("did not throw AccessDeniedException when session does not have write permissions on the property");
            } catch (AccessDeniedException ignored) {
            }
        }

        // let s3 upload threads finish
        Thread.sleep(5 * 1000);
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

    private static int httpPut(URL url, InputStream in) throws IOException  {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        OutputStream putStream = connection.getOutputStream();
        IOUtils.copy(in, putStream);
        putStream.close();
        return connection.getResponseCode();
    }
}
