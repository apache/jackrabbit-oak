package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.assertTrue;

public class UUIDConflictDetectorTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void testDetectConflicts() throws CommitFailedException, IOException {
        // Create source and target NodeStores
        MemoryNodeStore sourceStore = new MemoryNodeStore();
        MemoryNodeStore targetStore = new MemoryNodeStore();

        // Create nodes with UUIDs in source repository
        createNodeWithUUID(sourceStore, "/content", "1");
        createNodeWithUUID(sourceStore, "/content/foo", "2");
        createNodeWithUUID(sourceStore, "/content/foo/a", "5");
        createNodeWithUUID(sourceStore, "/content/foo/b", "3");
        createNodeWithUUID(sourceStore, "/content/bar", "4");
        createNodeWithUUID(sourceStore, "/content/bar/c", "6");
        createNodeWithUUID(sourceStore, "/content/bar/d", "7");
        createNodeWithUUID(sourceStore, "/content/aarpe/en/home/retirement/social-security/info-2022/best-reasons-not-to-take-benefits-early/jcr:content1", "abcd");

        // Create nodes with UUIDs in target repository
        createNodeWithUUID(targetStore, "/content", "1");
        createNodeWithUUID(targetStore, "/content/foo", "2");
        createNodeWithUUID(targetStore, "/content/foo/a", "5");
        createNodeWithUUID(targetStore, "/content/foo/f", "3");
        createNodeWithUUID(targetStore, "/content/bar", "4");
        createNodeWithUUID(targetStore, "/content/bar/c", "6");
        createNodeWithUUID(targetStore, "/content/bar/d", "7");
        createNodeWithUUID(targetStore, "/content/aarpe/en/home/social-security/best-reasons-not-to-take-benefits-early/jcr:content", "abcd");

        // Create UUIDConflictDetector and detect conflicts
        long timeStamp = Instant.now().toEpochMilli();
        UUIDConflictDetector detector = new UUIDConflictDetector(sourceStore, targetStore, temporaryFolder.getRoot(), timeStamp);
        detector.detectConflicts();

        // Check the output file for conflicts
        File conflictFile = new File("uuid_conflicts_" + timeStamp + ".txt");
        try (BufferedReader reader = new BufferedReader(new FileReader(conflictFile))) {
            String line;
            boolean hasConflict = false;
            while ((line = reader.readLine()) != null) {
                if (line.contains("vs")) {
                    hasConflict = true;
                    break;
                }
            }
            assertTrue(hasConflict);
        }
    }

    private void createNodeWithUUID(MemoryNodeStore store, String path, String uuid) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        createNodeWithUUID(builder, path, uuid);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void createNodeWithUUID(NodeBuilder builder, String path, String uuid) {
        for (String name : path.substring(1).split("/")) {
            builder = builder.child(name);
        }
        builder.setProperty("jcr:uuid", uuid, Type.STRING);
    }
}