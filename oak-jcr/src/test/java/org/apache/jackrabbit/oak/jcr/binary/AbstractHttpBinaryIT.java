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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import javax.jcr.Repository;
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
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordDirectAccessProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class with all the logic to test different data stores that support binaries with direct HTTP access */
public abstract class AbstractHttpBinaryIT extends AbstractRepositoryTest {
    protected static CloudStorageContainer s3Container = null;
    protected static CloudStorageContainer azureContainer = null;

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
            LOG.warn("WARN: Skipping AbstractHttpBinaryIT based test for S3 DataStore repo fixture because no S3 properties file was found given by 's3.config' system property or named 'aws.properties'.");
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
            LOG.warn("WARN: Skipping AbstractHttpBinaryIT based test for Azure DataStore repo fixture because no AZ properties file was found given by 'azure.config' system property or named 'azure.properties'.");
        }

        return fixtures;
    }

    protected static class CloudStorageContainer<T> {
        private final T client;
        private final String containerName;

        public CloudStorageContainer(T client, String containerName) {
            this.client = client;
            this.containerName = containerName;
        }

        public T getClient() { return client; }
        public String getContainerName() { return containerName; }
    }

    @Override
    protected Repository createRepository(NodeStore nodeStore) {
        Whiteboard wb = new DefaultWhiteboard();
        if (fixture instanceof BlobStoreHolder) {
            BlobStoreHolder holder = (BlobStoreHolder) fixture;
            BlobStore blobStore = holder.getBlobStore();
            if (blobStore instanceof BlobAccessProvider) {
                wb.register(BlobAccessProvider.class, (BlobAccessProvider) blobStore,
                        Collections.emptyMap());
            }
        }
        return initJcr(new Jcr(nodeStore).with(wb)).createRepository();
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

    protected ConfigurableDataRecordDirectAccessProvider getConfigurableHttpDataRecordProvider()
        throws RepositoryException {
        getRepository();
        if (fixture instanceof DataStoreHolder) {
            DataStore dataStore = ((DataStoreHolder) fixture).getDataStore();
            if (dataStore instanceof ConfigurableDataRecordDirectAccessProvider) {
                return (ConfigurableDataRecordDirectAccessProvider) dataStore;
            }
        }
        throw new AssertionError("issue with test setup, cannot retrieve underlying DataStore / ConfigurableDataRecordDirectAccessProvider");
    }

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

    protected interface BlobStoreHolder {
        BlobStore getBlobStore();
    }

    protected interface NodeStoreHolder {
        NodeStore getNodeStore();
    }

    protected interface FileStoreHolder {
        FileStore getFileStore();
        File getFileStoreRoot();
    }

    /**
     * Creates a repository with
     * - SegmentNodeStore, storing data in-memory
     * - an optional DataStore provided by DataStoreFixture
     */
    protected static class SegmentMemoryNodeStoreFixture extends NodeStoreFixture
            implements BlobStoreHolder, DataStoreHolder, NodeStoreHolder, FileStoreHolder {

        private final DataStoreFixture dataStoreFixture;

        private NodeStore nodeStore;

        private DataStore dataStore;

        private BlobStore blobStore;

        private FileStore fileStore;
        private File fileStoreRoot;

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

                    fileStoreRoot = createTempFolder();
                    FileStoreBuilder fileStoreBuilder = FileStoreBuilder.fileStoreBuilder(fileStoreRoot)
                        .withNodeDeduplicationCacheSize(16384)
                        .withMaxFileSize(256)
                        .withMemoryMapping(false);

                    if (dataStoreFixture != null) {

                        // create data store (impl specific)
                        dataStore = dataStoreFixture.createDataStore();

                        // init with a new folder inside a temporary one
                        dataStore.init(createTempFolder().getAbsolutePath());

                        blobStore = new DataStoreBlobStore(dataStore);
                        fileStoreBuilder.withBlobStore(blobStore);
                    }

                    fileStore = fileStoreBuilder.build();
                    nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

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

        @Override
        public BlobStore getBlobStore() {
            return blobStore;
        }

        @Override
        public NodeStore getNodeStore() {
            return nodeStore;
        }

        @Override
        public FileStore getFileStore() {
            return fileStore;
        }

        @Override
        public File getFileStoreRoot() {
            return fileStoreRoot;
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
    protected static class DocumentMemoryNodeStoreFixture extends NodeStoreFixture
            implements BlobStoreHolder, DataStoreHolder {

        private final DataStoreFixture dataStoreFixture;

        private NodeStore nodeStore;

        private DataStore dataStore;

        private BlobStore blobStore;

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

                        blobStore = new DataStoreBlobStore(dataStore);
                        documentNodeStoreBuilder.setBlobStore(blobStore);
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

        @Override
        public BlobStore getBlobStore() {
            return blobStore;
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
}
