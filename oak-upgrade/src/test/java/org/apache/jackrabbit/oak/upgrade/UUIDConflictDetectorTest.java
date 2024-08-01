package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class UUIDConflictDetectorTest {

//    @Test
//    public void testDetectConflicts() throws CommitFailedException {
//        // Create source and target NodeStores
//        MemoryNodeStore sourceStore = new MemoryNodeStore();
//        MemoryNodeStore targetStore = new MemoryNodeStore();
//
//        // Create nodes with UUIDs in source repository
//        createNodeWithUUID(sourceStore, "/content", "1");
//        createNodeWithUUID(sourceStore, "/content/foo", "2");
//        createNodeWithUUID(sourceStore, "/content/foo/a", "5");
//        createNodeWithUUID(sourceStore, "/content/foo/b", "3");
//        createNodeWithUUID(sourceStore, "/content/bar", "4");
//        createNodeWithUUID(sourceStore, "/content/bar/c", "6");
//        createNodeWithUUID(sourceStore, "/content/bar/d", "7");
//        createNodeWithUUID(sourceStore, "/content/aarpe/en/home/retirement/social-security/info-2022/best-reasons-not-to-take-benefits-early/jcr:content1", "abcd");
//
//        // Create nodes with UUIDs in target repository
//        createNodeWithUUID(targetStore, "/content", "1");
//        createNodeWithUUID(targetStore, "/content/foo", "2");
//        createNodeWithUUID(targetStore, "/content/foo/a", "5");
//        createNodeWithUUID(targetStore, "/content/foo/f", "3");
//        createNodeWithUUID(targetStore, "/content/bar", "4");
//        createNodeWithUUID(targetStore, "/content/bar/c", "6");
//        createNodeWithUUID(targetStore, "/content/bar/d", "7");
//        createNodeWithUUID(targetStore, "/content/aarpe/en/home/social-security/best-reasons-not-to-take-benefits-early/jcr:content", "abcd");
//
//        // Create UUIDConflictDetector and detect conflicts
//        UUIDConflictDetector detector = new UUIDConflictDetector(sourceStore, targetStore);
//        Map<String, Map.Entry<String, String>> conflicts = detector.detectConflicts();
//
//        conflicts.forEach((key, value) -> {
//            System.out.print("UUID: " + key + " : ");
//            System.out.print("sourcePath: " + value.getKey() + " ");
//            System.out.print("targetPath: " + value.getValue());
//            System.out.println();
//        });
//    }

//    @Test
//    public void testDetectConflictsAtPath() throws CommitFailedException {
//        // Create source and target NodeStores
//        MemoryNodeStore sourceStore = new MemoryNodeStore();
//        MemoryNodeStore targetStore = new MemoryNodeStore();
//
//        // Create nodes with UUIDs in source repository
//        createNodeWithUUID(sourceStore, "/content", "1");
//        createNodeWithUUID(sourceStore, "/content/foo", "2");
//        createNodeWithUUID(sourceStore, "/content/foo/a", "5");
//        createNodeWithUUID(sourceStore, "/content/foo/b", "3");
//        createNodeWithUUID(sourceStore, "/content/bar", "4");
//        createNodeWithUUID(sourceStore, "/content/bar/c", "6");
//        createNodeWithUUID(sourceStore, "/content/bar/d", "7");
//        createNodeWithUUID(sourceStore, "/content/aarpe/en/home/retirement/social-security/info-2022/best-reasons-not-to-take-benefits-early/jcr:content1", "abcd");
//
//        // Create nodes with UUIDs in target repository
//        createNodeWithUUID(targetStore, "/content", "1");
//        createNodeWithUUID(targetStore, "/content/foo", "2");
//        createNodeWithUUID(targetStore, "/content/foo/a", "5");
//        createNodeWithUUID(targetStore, "/content/foo/f", "3");
//        createNodeWithUUID(targetStore, "/content/bar", "4");
//        createNodeWithUUID(targetStore, "/content/bar/c", "6");
//        createNodeWithUUID(targetStore, "/content/bar/d", "7");
//        createNodeWithUUID(targetStore, "/content/aarpe/en/home/social-security/best-reasons-not-to-take-benefits-early/jcr:content", "abcd");
//
//        // Create UUIDConflictDetector and detect conflicts
//        UUIDConflictDetector detector = new UUIDConflictDetector(sourceStore, targetStore);
//        Map<String, Map.Entry<String, String>> conflicts = detector.detectConflicts("/content/aarpe");
//
//        conflicts.forEach((key, value) -> {
//            System.out.print("UUID: " + key + " : ");
//            System.out.print("sourcePath: " + value.getKey() + " ");
//            System.out.print("targetPath: " + value.getValue());
//            System.out.println();
//        });
//    }

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
        UUIDConflictDetector detector = new UUIDConflictDetector(sourceStore, targetStore);
        detector.detectConflicts();

        // Check the output file for conflicts
        File conflictFile = new File("uuid_conflicts_" + System.currentTimeMillis() + ".txt");
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

    @Test
    public void LCA() throws IOException {
        UUIDConflictDetector uuidConflictDetector = new UUIDConflictDetector(null, null);
        String[] includePath = {"/content/foo", "/content/foo/b", "/content/dam/c", "/content/dam/d", "/sys/setting"};
//        String[] includePath = {"/content/foo/b", "/content/foo/a", "/content/foo", "/home/user", "/sys/setting"};
        uuidConflictDetector.detectConflicts(includePath);
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