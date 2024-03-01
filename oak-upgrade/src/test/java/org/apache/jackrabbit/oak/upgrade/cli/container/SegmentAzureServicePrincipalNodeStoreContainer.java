package org.apache.jackrabbit.oak.upgrade.cli.container;

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.guava.common.io.Files;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils;

import java.io.File;
import java.io.IOException;

public class SegmentAzureServicePrincipalNodeStoreContainer implements NodeStoreContainer {
    private static final Environment ENVIRONMENT = new Environment();
    private static final String CONTAINER_NAME = "oak-migration-test";
    private static final String DIR = "repository";
    private static final String AZURE_SEGMENT_STORE_PATH = "https://%s.blob.core.windows.net/%s/%s";

    private final BlobStore blobStore;
    private FileStore fs;
    private File tmpDir;
    private AzurePersistence azurePersistence;

    public SegmentAzureServicePrincipalNodeStoreContainer() {
        this(null);
    }

    public SegmentAzureServicePrincipalNodeStoreContainer(BlobStore blobStore) {
        this.blobStore = blobStore;
    }


    @Override
    public NodeStore open() throws IOException {
        try {
            azurePersistence = createAzurePersistence();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        tmpDir = Files.createTempDir();
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(tmpDir)
                .withCustomPersistence(azurePersistence).withMemoryMapping(false);
        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        try {
            fs = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }

        return new FileStoreUtils.NodeStoreWithFileStore(SegmentNodeStoreBuilders.builder(fs).build(), fs);
    }

    private AzurePersistence createAzurePersistence() {
        if (azurePersistence != null) {
            return azurePersistence;
        }
        String path = String.format(AZURE_SEGMENT_STORE_PATH, ENVIRONMENT.getVariable(AzureUtilities.AZURE_ACCOUNT_NAME),
                CONTAINER_NAME, DIR);
        CloudBlobDirectory cloudBlobDirectory = ToolUtils.createCloudBlobDirectory(path, ENVIRONMENT);
        return new AzurePersistence(cloudBlobDirectory);
    }

    @Override
    public void close() {
        if (fs != null) {
            fs.close();
            fs = null;
        }
        if (tmpDir != null) {
            tmpDir.delete();
        }
    }

    @Override
    public void clean() throws IOException {
        AzurePersistence azurePersistence = createAzurePersistence();
        try {
            AzureUtilities.deleteAllBlobs(azurePersistence.getSegmentstoreDirectory());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getDescription() {
        return "az:" + String.format(AZURE_SEGMENT_STORE_PATH, ENVIRONMENT.getVariable(AzureUtilities.AZURE_ACCOUNT_NAME),
                CONTAINER_NAME, DIR);
    }
}