package org.apache.jackrabbit.oak.segment.azure.compat;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CloudBlobDirectoryTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;
    private CloudBlobDirectory blobDirectory;

    @Before
    public void setup() throws Exception {
        container = azurite.getContainer("oak-test");

        createSimpleStore("oak");

        // when listing files, we should not see this folder:
        createSimpleStore("not-oak");

        blobDirectory = container.getDirectoryReference("oak");
    }

    public void createSimpleStore(String directoryName) throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        AzurePersistence p = new AzurePersistence(container.getDirectoryReference(directoryName));

        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();

        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();
    }

    @Test
    public void directory() {
        assertEquals("oak", blobDirectory.getPrefix());
    }

    @Test
    public void listBlobs() {
        List<BlobItem> list = blobDirectory.listBlobs()
                .stream()
                .collect(Collectors.toList());

        assertTrue(list.stream().anyMatch(item -> item.getName().equals("oak/data00000a.tar/data00000a.tar.brf")));
        assertTrue(list.stream().anyMatch(item -> item.getName().equals("oak/journal.log.001")));
        assertTrue(list.stream().anyMatch(item -> item.getName().equals("oak/manifest")));

        // should not match other directories
        assertFalse(list.stream().anyMatch(item -> item.getName().equals("not-oak/manifest")));

    }

    @Test
    public void listBlobsStartingWith() {
        List<BlobItem> list = blobDirectory.listBlobsStartingWith("journal.log")
                .stream()
                .collect(Collectors.toList());

        assertTrue(list.stream().anyMatch(item -> item.getName().equals("oak/journal.log.001")));

        // should not match other directories
        assertFalse(list.stream().anyMatch(item -> item.getName().equals("not-oak/journal.log.001")));
    }

    @Test
    public void listBlobsWithOptions() {
        List<BlobItem> list = blobDirectory.listBlobs(new ListBlobsOptions().setPrefix("journal.log"), null)
                .stream()
                .collect(Collectors.toList());

        assertTrue(list.stream().anyMatch(item -> item.getName().equals("oak/journal.log.001")));

        // should not match other directories
        assertFalse(list.stream().anyMatch(item -> item.getName().equals("not-oak/journal.log.001")));
    }

    @Test
    public void getBlobClient() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        blobDirectory.getBlobClient("manifest").download(out);
        String actual = new String(out.toByteArray());

        assertTrue(actual.contains("store.version"));
    }

    @Test
    public void getBlobClientAbsolute() {
        String blobUrl = blobDirectory.getBlobClientAbsolute(new BlobItem().setName("oak/manifest")).getBlobUrl();
        System.out.println(blobUrl);
        assertTrue(blobUrl.endsWith("/oak%2Fmanifest"));
    }

    @Test
    public void getSubDirectory() {
        CloudBlobDirectory tarDirectory = blobDirectory.getSubDirectory("data00000a.tar");
        List<BlobItem> list = tarDirectory.listBlobs().stream().collect(Collectors.toList());

        assertTrue(list.stream().anyMatch(item -> item.getName().equals("oak/data00000a.tar/data00000a.tar.brf")));

        // should not match outside directories
        assertFalse(list.stream().anyMatch(item -> item.getName().equals("oak/journal.log.001")));
    }

    @Test
    public void deleteBlobIfExists() {
        // Test: no exception thrown
        blobDirectory.deleteBlobIfExists(blobDirectory.getBlobClient("not-existing-blob"));
    }

    @Test
    public void getUri() {
        URI uri = blobDirectory.getUri();
        System.out.println(uri);
        assertTrue(uri.toString().endsWith("/oak"));
    }

    @Test
    public void getContainerName() {
        assertTrue(blobDirectory.getContainerName().contains("oak"));
    }

    @Test
    public void getPrefix() {
        assertEquals("oak", blobDirectory.getPrefix());
    }

    @Test
    public void getFilename() {
        CloudBlobDirectory subDirectory = blobDirectory.getSubDirectory("my-dir");
        assertEquals("my-dir", subDirectory.getFilename());
    }

}