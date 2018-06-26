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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketAccelerateStatus;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.ConfigurableHttpDataRecordProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.AfterClass;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class with all the logic to test different data stores that support binaries with direct HTTP access */
public abstract class AbstractHttpBinaryIT extends AbstractRepositoryTest {
    private static CloudStorageContainer s3Container = null;
    private static CloudStorageContainer azureContainer = null;

    protected static Logger LOG = LoggerFactory.getLogger(AbstractHttpBinaryIT.class);

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        // could add Azure Blob store as another test case here later

        Collection<NodeStoreFixture> fixtures = new ArrayList<>();

        // only add fixtures for DataStore implementations if they are configured

        Properties s3Props = S3DataStoreFixture.loadS3Properties();
        if (s3Props != null) {
            // Create a container with transfer acceleration set
            // Will be cleaned up at test completion
            String bucketName = "direct-binary-test-" + UUID.randomUUID().toString();
            s3Props.setProperty(S3Constants.S3_BUCKET, bucketName);
            AmazonS3 client = org.apache.jackrabbit.oak.blob.cloud.s3.Utils.openService(s3Props);
            s3Container = new CloudStorageContainer<AmazonS3>(client, bucketName);
            CreateBucketRequest req = new CreateBucketRequest(bucketName);
            client.createBucket(req);
            SetBucketAccelerateConfigurationRequest accelReq =
                    new SetBucketAccelerateConfigurationRequest(bucketName,
                            new BucketAccelerateConfiguration(
                                    BucketAccelerateStatus.Enabled
                            )
                    );
            client.setBucketAccelerateConfiguration(accelReq);

            S3DataStoreFixture s3 = new S3DataStoreFixture(s3Props);
            fixtures.add(new SegmentMemoryNodeStoreFixture(s3));
            fixtures.add(new DocumentMemoryNodeStoreFixture(s3));
        } else {
            LOG.warn("WARN: Skipping AbstractURLBinaryIT based test for S3 DataStore repo fixture because no S3 properties file was found given by 's3.config' system property or named 'aws.properties'.");
        }

        Properties azProps = AzureDataStoreFixture.loadAzureProperties();
        if (azProps != null) {
            // Create container
            // Will be cleaned up at test completion
            String containerName = "direct-binary-test-" + UUID.randomUUID().toString();
            azProps.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, containerName);
            String connectionString = org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils.getConnectionStringFromProperties(azProps);
            try {
                CloudBlobContainer container = org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils.getBlobContainer(connectionString, containerName);
                container.createIfNotExists();
                azureContainer = new CloudStorageContainer<CloudBlobContainer>(container, containerName);
            }
            catch (DataStoreException | StorageException e) {
                // If exception is thrown, container is not created, and we can't do the Azure test
                // Set azProps to null so the test is not executed
                azProps = null;
            }
        }
        if (azProps != null) {
            AzureDataStoreFixture azure = new AzureDataStoreFixture(azProps);
            fixtures.add(new SegmentMemoryNodeStoreFixture(azure));
            fixtures.add(new DocumentMemoryNodeStoreFixture(azure));
        } else {
            LOG.warn("WARN: Skipping AbstractURLBinaryIT based test for Azure DataStore repo fixture because no AZ properties file was found given by 'azure.config' system property or named 'azure.properties'.");
        }

        return fixtures;
    }

    private static class CloudStorageContainer<T> {
        private final T client;
        private final String containerName;

        public CloudStorageContainer(T client, String containerName) {
            this.client = client;
            this.containerName = containerName;
        }

        public T getClient() { return client; }
        public String getContainerName() { return containerName; }
    }

    @AfterClass
    public static void destroyTestContainers() {
        if (null != s3Container) {
            AmazonS3 client = (AmazonS3) s3Container.getClient();
            String bucketName = s3Container.getContainerName();
            ObjectListing listing = client.listObjects(bucketName);

            // For S3, you have to empty the bucket before removing the bucket itself
            while (true) {
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    client.deleteObject(bucketName, summary.getKey());
                }
                if (! listing.isTruncated()) {
                    break;
                }
                listing = client.listNextBatchOfObjects(listing);
            }
            client.deleteBucket(bucketName);
        }
        if (null != azureContainer) {
            try {
                // For Azure, you can just delete the container and all
                // blobs it in will also be deleted
                ((CloudBlobContainer) azureContainer.getClient()).delete();
            }
            catch (StorageException e) {
                LOG.warn("Unable to delete Azure Blob container {}", azureContainer.getContainerName());
            }
        }
    }

    protected AbstractHttpBinaryIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    protected ConfigurableHttpDataRecordProvider getConfigurableHttpDataRecordProvider()
        throws RepositoryException {
        getRepository();
        if (fixture instanceof DataStoreHolder) {
            DataStore dataStore = ((DataStoreHolder) fixture).getDataStore();
            if (dataStore instanceof ConfigurableHttpDataRecordProvider) {
                return (ConfigurableHttpDataRecordProvider) dataStore;
            }
        }
        throw new AssertionError("issue with test setup, cannot retrieve underlying DataStore / ConfigurableHttpDataRecordProvider");
    }

    // -----< useful constants >----------------------------------------------------------------------------------------

    protected static final int MB = 1024 * 1024;
    protected static final int SECONDS = 1000;

    // -----< data store fixtures >-------------------------------------------------------------------------------------

    protected interface DataStoreFixture {
        DataStore createDataStore();
    }

    protected static class S3DataStoreFixture implements DataStoreFixture {

        private final Properties s3Props;

        public S3DataStoreFixture(Properties s3Props) {
            this.s3Props = s3Props;
        }

        @Nullable
        public static Properties loadS3Properties() {
            return loadDataStoreProperties("s3.config", "aws.properties", ".aws");
        }

        @Override
        public DataStore createDataStore() {
            S3DataStore dataStore = new S3DataStore();
            dataStore.setProperties(s3Props);
            return dataStore;
        }
    }

    protected static class AzureDataStoreFixture implements DataStoreFixture {

        private final Properties azProps;

        public AzureDataStoreFixture(Properties azProps) {
            this.azProps = azProps;
        }

        @Nullable
        public static Properties loadAzureProperties() {
            return loadDataStoreProperties("azure.config", "azure.properties", ".azure");
        }

        @Override
        public DataStore createDataStore() {
            AzureDataStore dataStore = new AzureDataStore();
            dataStore.setProperties(azProps);
            return dataStore;
        }
    }

    @Nullable
    protected static Properties loadDataStoreProperties(String systemProperty, String defaultFileName, String homeFolderName) {
        Properties props = new Properties();
        try {
            File file = new File(System.getProperty(systemProperty, defaultFileName));
            if (!file.exists()) {
                file = Paths.get(System.getProperty("user.home"), homeFolderName, defaultFileName).toFile();
            }
            props.load(new FileReader(file));
        } catch (IOException e) {
            return null;
        }
        return props;
    }

    // -----< repository fixtures >-------------------------------------------------------------------------------------

    protected interface DataStoreHolder {
        DataStore getDataStore();
    }

    /**
     * Creates a repository with
     * - SegmentNodeStore, storing data in-memory
     * - an optional DataStore provided by DataStoreFixture
     */
    protected static class SegmentMemoryNodeStoreFixture extends NodeStoreFixture implements DataStoreHolder {

        private final DataStoreFixture dataStoreFixture;

        private NodeStore nodeStore;

        private DataStore dataStore;

        public SegmentMemoryNodeStoreFixture(@Nullable DataStoreFixture dataStoreFixture) {
            this.dataStoreFixture = dataStoreFixture;
        }

        @Override
        public NodeStore createNodeStore() {
            // HACK: reuse repo for multiple tests so they are much faster
            // this forces tests to properly clean their test content
            if (nodeStore == null) {
                try {
                    System.out.println();
                    System.out.println("----------------- creating repository using " + toString() + " -----------------");

                    FileStoreBuilder fileStoreBuilder = FileStoreBuilder.fileStoreBuilder(createTempFolder())
                        .withNodeDeduplicationCacheSize(16384)
                        .withMaxFileSize(256)
                        .withMemoryMapping(false);

                    if (dataStoreFixture != null) {

                        // create data store (impl specific)
                        dataStore = dataStoreFixture.createDataStore();

                        // init with a new folder inside a temporary one
                        dataStore.init(createTempFolder().getAbsolutePath());

                        fileStoreBuilder.withBlobStore(new DataStoreBlobStore(dataStore));
                    }

                    nodeStore = SegmentNodeStoreBuilders.builder(fileStoreBuilder.build()).build();

                } catch (IOException | InvalidFileStoreVersionException | RepositoryException e) {
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
            String nodeStoreName = getClass().getSimpleName();
            String name = StringUtils.removeEnd(nodeStoreName, "Fixture");
            if (dataStoreFixture != null) {
                String dataStoreName = dataStoreFixture.getClass().getSimpleName();
                return name + "_" + StringUtils.removeEnd(dataStoreName, "Fixture");
            }
            return name;
        }
    }

    /**
     * Creates a repository with
     * - DocumentNodeStore, storing data in-memory
     * - an optional DataStore provided by DataStoreFixture
     */
    protected static class DocumentMemoryNodeStoreFixture extends NodeStoreFixture implements DataStoreHolder {

        private final DataStoreFixture dataStoreFixture;

        private NodeStore nodeStore;

        private DataStore dataStore;

        public DocumentMemoryNodeStoreFixture(@Nullable DataStoreFixture dataStoreFixture) {
            this.dataStoreFixture = dataStoreFixture;
        }

        @Override
        public NodeStore createNodeStore() {
            // HACK: reuse repo for multiple tests so they are much faster
            // this forces tests to properly clean their test content
            if (nodeStore == null) {
                try {
                    System.out.println();
                    System.out.println("----------------- creating repository using " + toString() + " -----------------");

                    DocumentNodeStoreBuilder<?> documentNodeStoreBuilder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();

                    if (dataStoreFixture != null) {

                        // create data store (impl specific)
                        dataStore = dataStoreFixture.createDataStore();

                        // init with a new folder inside a temporary one
                        dataStore.init(createTempFolder().getAbsolutePath());

                        documentNodeStoreBuilder.setBlobStore(new DataStoreBlobStore(dataStore));
                    }

                    nodeStore = documentNodeStoreBuilder.build();

                } catch (IOException | RepositoryException e) {
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
            String nodeStoreName = getClass().getSimpleName();
            String name = StringUtils.removeEnd(nodeStoreName, "Fixture");
            if (dataStoreFixture != null) {
                String dataStoreName = dataStoreFixture.getClass().getSimpleName();
                return name + "_" + StringUtils.removeEnd(dataStoreName, "Fixture");
            }
            return name;
        }
    }

    // -----< temp folder management >----------------------------------------------------------------------------------

    private static Collection<File> tempFoldersToDelete = new ArrayList<>();

    public static File createTempFolder() throws IOException {
        // create temp folder inside maven's "target" folder
        return createTempFolder(new File("target"));
    }

    public static File createTempFolder(File parent) throws IOException {
        File tempFolder = File.createTempFile("junit", "", parent);
        tempFolder.delete();
        tempFolder.mkdir();
        tempFoldersToDelete.add(tempFolder);
        return tempFolder;
    }

    // HACK: delete data store folder at the end of this test class, as we reuse the NodeStore
    // in the NodeStoreFixture and NodeStoreFixture.dispose() would delete between test runs
    @AfterClass
    public static void cleanupStores() throws IOException {
        for (File folder : tempFoldersToDelete) {
            FileUtils.deleteDirectory(folder);
        }
        tempFoldersToDelete.clear();
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

    protected int httpPut(@Nullable URL url, long contentLength, InputStream in) throws IOException {
        return httpPut(url, contentLength, in, false);
    }

    /**
     * Uploads data via HTTP put to the provided URL.
     *
     * @param url The URL to upload to.
     * @param contentLength Value to set in the Content-Length header.
     * @param in - The input stream to upload.
     * @param isMultiPart - True if this upload is part of a multi-part upload.
     * @return HTTP response code from the upload request.  Note that a successful
     * response for S3 is 200 - OK whereas for Azure it is 201 - Created.
     * @throws IOException
     */
    protected int httpPut(@Nullable URL url, long contentLength, InputStream in, boolean isMultiPart) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(url);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("Content-Length", String.valueOf(contentLength));
        connection.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));

        OutputStream putStream = connection.getOutputStream();
        IOUtils.copy(in, putStream);
        putStream.close();
        return connection.getResponseCode();
    }

    boolean isSuccessfulHttpPut(int code, ConfigurableHttpDataRecordProvider dataStore) {
        if (dataStore instanceof S3DataStore) {
            return 200 == code;
        }
        else if (dataStore instanceof AzureDataStore) {
            return 201 == code;
        }
        return 200 == code;
    }

    boolean isFailedHttpPut(int code) {
        return code >= 400 && code < 500;
    }

    protected int httpPutTestStream(URL url) throws IOException {
        String content = "hello world";
        return httpPut(url, content.getBytes().length, getTestInputStream(content));
    }

    protected InputStream httpGet(@Nullable URL url) throws IOException  {
        // this weird combination of @Nullable and assertNotNull() is for IDEs not warning in test methods
        assertNotNull(url);

        URLConnection conn = url.openConnection();
        return conn.getInputStream();
    }
}
