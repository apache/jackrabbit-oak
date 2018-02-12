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

import static org.junit.Assert.assertNotNull;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.URLReadableDataStore;
import org.apache.jackrabbit.oak.spi.blob.URLWritableDataStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.runners.Parameterized;

/** Base class with all the logic to test different data stores that support URL writable or readable binaries */
public abstract class AbstractURLBinaryIT extends AbstractRepositoryTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        // could add Azure Blob store as another test case here later

        Collection<NodeStoreFixture> fixtures = new ArrayList<>();
        // only add fixture if configured
        Properties s3Props = S3DataStoreWithMemorySegmentFixture.loadS3Properties();
        if (s3Props != null) {
            fixtures.add(new S3DataStoreWithMemorySegmentFixture(s3Props));
        } else {
            System.out.println("Skipping AbstractURLBinaryIT based test for S3 repo fixture as no S3 properties file found given by 's3.config' system property or named 'aws.properties'.");
        }
        return fixtures;
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

    private interface URLReadableDataStoreFixture {
        URLReadableDataStore getURLReadableDataStore();
    }

    protected URLWritableDataStore getURLWritableDataStore() throws RepositoryException {
        // ensure fixture has created repo
        getRepository();
        if (fixture instanceof URLWritableDataStoreFixture) {
            return ((URLWritableDataStoreFixture) fixture).getURLWritableDataStore();
        }
        throw new AssertionError("issue with test setup, cannot retrieve underlying URLWritableDataStore");
    }

    protected URLReadableDataStore getURLReadableDataStore() throws RepositoryException {
        // ensure fixture has created repo
        getRepository();
        if (fixture instanceof URLReadableDataStoreFixture) {
            return ((URLReadableDataStoreFixture) fixture).getURLReadableDataStore();
        }
        throw new AssertionError("issue with test setup, cannot retrieve underlying URLWritableDataStore");
    }

    // for this integration test create a SegmentNodeStore with
    // - segments in memory
    // - S3 data store as blob store
    // - S3 configuration from "aws.properties" file or "-Ds3.config=<filename>" system property
    protected static class S3DataStoreWithMemorySegmentFixture extends NodeStoreFixture
        implements URLWritableDataStoreFixture, URLReadableDataStoreFixture {

        private final Properties s3Props;

        private NodeStore nodeStore;

        private S3DataStore s3DataStore;

        // track create temp folder to delete them in dispose()
        // note this assumes the fixture is only used once to createNodeStore()
        private File dataStoreFolder;

        public S3DataStoreWithMemorySegmentFixture(Properties s3Props) {
            this.s3Props = s3Props;
        }

        @Nullable
        public static Properties loadS3Properties() {
            Properties s3Props = new Properties();
            try {
                s3Props.load(new FileReader(System.getProperty("s3.config", "aws.properties")));
            } catch (IOException e) {
                return null;
            }
            return s3Props;
        }

        @Override
        public NodeStore createNodeStore() {
            // HACK: reuse repo for multiple tests so they are much faster
            // this forces tests to properly clean their test content, and might create issues with dispose() below
            // which deletes the data store directory after each test run, although that seems to be ok
            if (nodeStore == null) {
                System.out.println("-------------------------- creating S3 backed repo --------------------------");
                try {
                    nodeStore = SegmentNodeStoreBuilders.builder(new MemoryStore() {

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
                    throw new AssertionError("Cannot create test repo fixture " + toString(), e);
                }
            }
            return nodeStore;
        }

        public BlobStore createBlobStore() {
            try {
                // create data store disk cache inside maven's "target" folder
                dataStoreFolder = createTempFolder(new File("target"));

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

        @Override
        public URLReadableDataStore getURLReadableDataStore() {
            return s3DataStore;
        }
    }

    // -----< test helpers >--------------------------------------------------------------------------------------------

    protected void waitForUploads() throws InterruptedException {
        // let data store upload threads finish
        Thread.sleep(5 * SECONDS);
    }

    protected static InputStream getTestInputStream(String content) {
        try {
            return new ByteArrayInputStream(content.getBytes("utf-8"));
        } catch (UnsupportedEncodingException unexpected) {
            unexpected.printStackTrace();
            // return empty stream
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    protected static InputStream getTestInputStream(int size) {
        byte[] blob = new byte[size];
        // magic bytes so it's not just all zeros
        blob[0] = 1;
        blob[1] = 2;
        blob[2] = 3;
        return new ByteArrayInputStream(blob);
    }

    protected int httpPut(@Nullable URL url, InputStream in) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(url);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        OutputStream putStream = connection.getOutputStream();
        IOUtils.copy(in, putStream);
        putStream.close();
        return connection.getResponseCode();
    }

    protected int httpPutTestStream(URL url) throws IOException {
        return httpPut(url, getTestInputStream("hello world"));
    }

    protected InputStream httpGet(@Nullable URL url) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(url);

        URLConnection conn = url.openConnection();
        return conn.getInputStream();
    }
}
