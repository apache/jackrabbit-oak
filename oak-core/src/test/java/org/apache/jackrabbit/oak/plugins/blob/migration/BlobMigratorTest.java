package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.SplitBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class BlobMigratorTest {

    private static final int LENGTH = 1024;

    private static final Random RANDOM = new Random();

    private SegmentStore segmentStore;

    private NodeStore nodeStore;

    private BlobMigrator migrator;

    private BlobStore newBlobStore;

    @Before
    public void setup() throws CommitFailedException, IllegalArgumentException, IOException {
        final File repository = Files.createTempDir();
        final File segmentDir = new File(repository, "segmentstore");
        final BlobStore oldBlobStore = new FileBlobStore(repository.getPath() + "/old");
        createContent(oldBlobStore, segmentDir);

        newBlobStore = new FileBlobStore(repository.getPath() + "/new");
        final SplitBlobStore splitBlobStore = new SplitBlobStore(repository.getPath(), oldBlobStore, newBlobStore);
        segmentStore = FileStore.newFileStore(segmentDir).withBlobStore(splitBlobStore).create();
        nodeStore = SegmentNodeStore.newSegmentNodeStore(segmentStore).create();
        migrator = new BlobMigrator(splitBlobStore, nodeStore);
    }

    @After
    public void teardown() {
        segmentStore.close();
    }

    @Test
    public void testMigrate() throws IOException, CommitFailedException {
        migrator.migrate();

    }

    private static void createContent(BlobStore blobStore, File segmentDir) throws IOException, CommitFailedException {
        SegmentStore segmentStore = FileStore.newFileStore(segmentDir).withBlobStore(blobStore).create();
        NodeStore nodeStore = SegmentNodeStore.newSegmentNodeStore(segmentStore).create();
        final NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        rootBuilder.child("node1").setProperty("prop", createBlob(nodeStore));
        rootBuilder.child("node2").setProperty("prop", createBlob(nodeStore));
        final PropertyBuilder<Blob> builder = PropertyBuilder.array(Type.BINARY, "prop");
        builder.addValue(createBlob(nodeStore));
        builder.addValue(createBlob(nodeStore));
        builder.addValue(createBlob(nodeStore));
        rootBuilder.child("node3").setProperty(builder.getPropertyState());
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        segmentStore.close();
    }

    private static Blob createBlob(NodeStore nodeStore) throws IOException {
        byte[] buffer = new byte[LENGTH];
        RANDOM.nextBytes(buffer);
        return nodeStore.createBlob(new ByteArrayInputStream(buffer));
    }
}
