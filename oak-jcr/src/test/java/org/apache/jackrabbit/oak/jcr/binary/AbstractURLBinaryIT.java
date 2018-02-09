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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.CheckForNull;
import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.runners.Parameterized;

/** Base class with all the logic to test different data stores that support URL writable or readable binaries */
public abstract class AbstractURLBinaryIT extends AbstractRepositoryTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        // could add Azure Blob store as another test case here later
        return Collections.singletonList(new S3DataStoreWithMemorySegmentFixture());
    }

    protected AbstractURLBinaryIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    // -----< useful constants >----------------------------------------------------------------------------------------

    protected static final int MB = 1024 * 1024;
    protected static final int SECONDS = 1000;

    // -----< fixtures >------------------------------------------------------------------------------------------------

    // abstraction so we can test different data store implementations using separate fixtures
    // but control common configuration elements such as expiry times
    private interface URLWritableDataStoreFixture {
        URLWritableDataStore getURLWritableDataStore();
    }

    protected URLWritableDataStore getDataStore() throws RepositoryException {
        // ensure fixture has created repo
        getRepository();
        if (fixture instanceof URLWritableDataStoreFixture) {
            return ((URLWritableDataStoreFixture) fixture).getURLWritableDataStore();
        }
        throw new AssertionError("issue with test setup, cannot retrieve underlying URLWritableDataStore");
    }

    // for this integration test create a SegmentNodeStore with
    // - segments in memory
    // - S3 data store as blob store
    // - S3 configuration from "s3.properties" file or "-Ds3.config=<filename>" system property
    private static class S3DataStoreWithMemorySegmentFixture extends NodeStoreFixture implements URLWritableDataStoreFixture {

        private S3DataStore s3DataStore;

        // track create temp folder to delete them in dispose()
        // note this assumes the fixture is only used once to createNodeStore()
        private File dataStoreFolder;

        @Override
        public NodeStore createNodeStore() {
            try {
                return SegmentNodeStoreBuilders.builder(new MemoryStore() {

                    private BlobStore blobStore;

                    @CheckForNull
                    @Override
                    public BlobStore getBlobStore() {
                        if (blobStore == null) {
                            blobStore = createBlobStore();
                        }
                        return blobStore;
                    }

                }).build();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        public BlobStore createBlobStore() {
            try {
                // create data store disk cache inside maven's "target" folder
                dataStoreFolder = createTempFolder(new File("target"));

                // read S3 config file
                Properties s3Props = new Properties();
                s3Props.load(new FileReader(System.getProperty("s3.config", "s3.properties")));

                // create S3 DS
                s3DataStore = new S3DataStore();
                s3DataStore.setProperties(s3Props);

                // init with a new folder inside a temporary one
                s3DataStore.init(dataStoreFolder.getAbsolutePath());

                return new DataStoreBlobStore(s3DataStore);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        private File createTempFolder(File parent) throws IOException {
            File tempFolder = File.createTempFile("junit", "", parent);
            tempFolder.delete();
            tempFolder.mkdir();
            return tempFolder;
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            try {
                FileUtils.deleteDirectory(dataStoreFolder);
            } catch (IOException e) {
                System.out.println("Could not cleanup temp folder after test: " + e.getMessage());
            }
        }

        // for nice Junit parameterized test labels
        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        @Override
        public URLWritableDataStore getURLWritableDataStore() {
            return s3DataStore;
        }
    }

    // -----< test helpers >--------------------------------------------------------------------------------------------

    protected void waitForUploads() throws InterruptedException {
        // let data store upload threads finish
        Thread.sleep(5 * SECONDS);
    }

    protected static int httpPut(URL url, InputStream in) throws IOException  {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        OutputStream putStream = connection.getOutputStream();
        IOUtils.copy(in, putStream);
        putStream.close();
        return connection.getResponseCode();
    }
}
