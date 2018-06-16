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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.ConfigurableHttpDataRecordProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.AfterClass;
import org.junit.runners.Parameterized;

/** Base class with all the logic to test different data stores that support URL writable or readable binaries */
public abstract class AbstractHttpBinaryIT extends AbstractRepositoryTest {

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

    private static File staticDataStoreFolder;
    private static File staticFileStoreFolder;

    // HACK: delete data store folder at the end of this test class, as we reuse the NodeStore
    // in the NodeStoreFixture and NodeStoreFixture.dispose() would delete between test runs
    @AfterClass
    public static void cleanupStores() throws IOException {
        if (staticDataStoreFolder != null) {
            FileUtils.deleteDirectory(staticDataStoreFolder);
        }
        if (staticFileStoreFolder != null) {
            FileUtils.deleteDirectory(staticFileStoreFolder);
        }
    }

    protected AbstractHttpBinaryIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    protected ConfigurableHttpDataRecordProvider getConfigurableHttpDataRecordProvider()
        throws RepositoryException {
        getRepository();
        if (fixture instanceof DataStoreFixture) {
            DataStore dataStore = ((DataStoreFixture) fixture).getDataStore();
            if (dataStore instanceof ConfigurableHttpDataRecordProvider) {
                return (ConfigurableHttpDataRecordProvider) dataStore;
            }
        }
        throw new AssertionError("issue with test setup, cannot retrieve underlying DataStore / ConfigurableHttpDataRecordProvider");
    }

    // -----< useful constants >----------------------------------------------------------------------------------------

    protected static final int MB = 1024 * 1024;
    protected static final int SECONDS = 1000;

    // -----< fixtures >------------------------------------------------------------------------------------------------

    // abstraction so we can test different data store implementations using separate fixtures
    // but control common configuration elements such as expiry times
    protected interface DataStoreFixture {
        DataStore getDataStore();
    }

    protected static abstract class MemorySegmentWithDataStoreFixture extends NodeStoreFixture implements DataStoreFixture {

        protected final Properties props;

        private NodeStore nodeStore;

        private DataStore dataStore;

        public MemorySegmentWithDataStoreFixture(Properties props) {
            this.props = props;
        }

        protected abstract DataStore createDataStore();

        @Override
        public NodeStore createNodeStore() {
            // HACK: reuse repo for multiple tests so they are much faster
            // this forces tests to properly clean their test content
            if (nodeStore == null) {
                try {
                    dataStore = createDataStore();
                    BlobStore blobStore = new DataStoreBlobStore(dataStore);

                    staticFileStoreFolder = createTempFolder(new File("target"));
                    nodeStore = SegmentNodeStoreBuilders.builder(
                        FileStoreBuilder.fileStoreBuilder(staticFileStoreFolder)
                            .withNodeDeduplicationCacheSize(16384)
                            .withBlobStore(blobStore)
                            .withMaxFileSize(256)
                            .withMemoryMapping(false)
                            .build()
                    ).build();
                } catch (IOException | InvalidFileStoreVersionException e) {
                    throw new AssertionError("Cannot create test repo fixture " + toString(), e);
                }
            }
            return nodeStore;
        }

        @Override
        public DataStore getDataStore() {
            return dataStore;
        }

        // for nice Junit parameterized test labels
        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    // for this integration test create a SegmentNodeStore with
    // - segments in memory
    // - S3 data store as blob store
    // - S3 configuration from "aws.properties" file or "-Ds3.config=<filename>" system property
    protected static class S3DataStoreWithMemorySegmentFixture extends MemorySegmentWithDataStoreFixture {

        public S3DataStoreWithMemorySegmentFixture(Properties s3Props) {
            super(s3Props);
        }

        @Nullable
        public static Properties loadS3Properties() {
            Properties s3Props = new Properties();
            try {
                File awsProps = new File(System.getProperty("s3.config", "aws.properties"));
                if (!awsProps.exists()) {
                    awsProps = Paths.get(System.getProperty("user.home"), ".aws", "aws.properties").toFile();
                }
                s3Props.load(new FileReader(awsProps));
            } catch (IOException e) {
                return null;
            }
            return s3Props;
        }

        @Override
        public DataStore createDataStore() {
            System.out.println("-------------------------- creating S3 backed repo --------------------------");

            try {
                // create data store disk cache inside maven's "target" folder
                File dataStoreFolder = createTempFolder(new File("target"));
                staticDataStoreFolder = dataStoreFolder;

                // create S3 DS
                S3DataStore dataStore = new S3DataStore();
                dataStore.setProperties(props);

                // init with a new folder inside a temporary one
                dataStore.init(dataStoreFolder.getAbsolutePath());

                return dataStore;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static File createTempFolder(File parent) throws IOException {
        File tempFolder = File.createTempFile("junit", "", parent);
        tempFolder.delete();
        tempFolder.mkdir();
        return tempFolder;
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
