package org.apache.jackrabbit.oak.upgrade;

import org.apache.commons.lang3.StringUtils;
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
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UUIDConflictDetectorTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void testConflicts() throws CommitFailedException, IOException {
        Set<String> conflictingUUIDs = new HashSet<>() {{
            add("3");
            add("4");
            add("7");
            add("9");
        }};

        Set<String> conflictingSourcePaths = new HashSet<>() {{
            add("/content/b'");
            add("/content/foo/a'");
            add("/content/bar/e'");
            add("/content/bar/e'/g");
        }};

        Set<String> conflictingTargetPaths = new HashSet<>() {{
            add("/content/foo/a");
            add("/content/foo/b");
            add("/content/bar/e");
            add("/content/bar/e/g");
        }};

        testConflicts(null, conflictingUUIDs, conflictingSourcePaths, conflictingTargetPaths);
    }

    @Test
    public void testConflictsWithIncludePaths() throws CommitFailedException, IOException {
        String[] includePaths = new String[]{
                "/content/foo/c",
                "/content/foo/a'",
                "/content/foo/c",
                "/content/foo",
                "/content/bar/e'",
                "/content/bar/e'/g",
        };

        Set<String> conflictingUUIDs = new HashSet<>() {{
            add("3");
            add("7");
            add("9");
        }};

        Set<String> conflictingSourcePaths = new HashSet<>() {{
            add("/content/foo/a'");
            add("/content/bar/e'");
            add("/content/bar/e'/g");
        }};

        Set<String> conflictingTargetPaths = new HashSet<>() {{
            add("/content/foo/a");
            add("/content/foo/b");
            add("/content/bar/e");
            add("/content/bar/e/g");
        }};

        testConflicts(includePaths, conflictingUUIDs, conflictingSourcePaths, conflictingTargetPaths);

    }

    @Test
    public void noConflicts() throws CommitFailedException, IOException {
        MemoryNodeStore sourceStore = new MemoryNodeStore();
        MemoryNodeStore targetStore = new MemoryNodeStore();
        // Create nodes with UUIDs in source repository
        createNodeWithUUID(sourceStore, "/content", "1");
        createNodeWithUUID(sourceStore, "/content/foo", "2");
        createNodeWithUUID(sourceStore, "/content/foo/a", "3");
        createNodeWithUUID(sourceStore, "/content/b", null);
        createNodeWithUUID(sourceStore, "/content/bar", "4");
        createNodeWithUUID(sourceStore, "/content/bar/d", "5");

        // Create nodes with UUIDs in target repository
        createNodeWithUUID(targetStore, "/content", "1");
        createNodeWithUUID(targetStore, "/content/foo", "2");
        createNodeWithUUID(targetStore, "/content/foo/a", "3");
        createNodeWithUUID(targetStore, "/content/bar", "4");

        long timeStamp = Instant.now().toEpochMilli();

        UUIDConflictDetector detector = new UUIDConflictDetector(sourceStore, targetStore, temporaryFolder.getRoot(), timeStamp);
        detector.detectConflicts(null);

        File conflictFile = new File(temporaryFolder.getRoot(), "uuid_conflicts_" + timeStamp + ".txt");
        assertEquals(0L, conflictFile.length());
    }

    private void testConflicts(String[] includePaths, Set<String> conflictingUUIDs, Set<String> conflictingSourcePaths, Set<String> conflictingTargetPaths) throws CommitFailedException, IOException {
        MemoryNodeStore sourceStore = new MemoryNodeStore();
        MemoryNodeStore targetStore = new MemoryNodeStore();

        // Create nodes with UUIDs in source repository
        createNodeWithUUID(sourceStore, "/content", "1");
        createNodeWithUUID(sourceStore, "/content/foo", "2");
        createNodeWithUUID(sourceStore, "/content/foo/a'", "3");
        createNodeWithUUID(sourceStore, "/content/b'", "4");
        createNodeWithUUID(sourceStore, "/content/foo/c", "5");
        createNodeWithUUID(sourceStore, "/content/foo/c/d", "6");
        createNodeWithUUID(sourceStore, "/content/bar", null);
        createNodeWithUUID(sourceStore, "/content/bar/e'", "7");
        createNodeWithUUID(sourceStore, "/content/bar/f", "8");
        createNodeWithUUID(sourceStore, "/content/bar/e'/g", "9");

        // Create nodes with UUIDs in target repository
        createNodeWithUUID(targetStore, "/content", "1");
        createNodeWithUUID(targetStore, "/content/foo", "2");
        createNodeWithUUID(targetStore, "/content/foo/a", "3");
        createNodeWithUUID(targetStore, "/content/foo/b", "4");
        createNodeWithUUID(targetStore, "/content/foo/c", "5");
        createNodeWithUUID(targetStore, "/content/foo/c/d", "6");
        createNodeWithUUID(targetStore, "/content/bar", null);
        createNodeWithUUID(targetStore, "/content/bar/e", "7");
        createNodeWithUUID(targetStore, "/content/bar/f", "8");
        createNodeWithUUID(targetStore, "/content/bar/e/g", "9");

        long timeStamp = Instant.now().toEpochMilli();

        UUIDConflictDetector detector = new UUIDConflictDetector(sourceStore, targetStore, temporaryFolder.getRoot(), timeStamp);
        detector.detectConflicts(includePaths);


        // Check the output file for conflicts
        File conflictFile = new File(temporaryFolder.getRoot(), "uuid_conflicts_" + timeStamp + ".txt");
        try (BufferedReader reader = new BufferedReader(new FileReader(conflictFile))) {
            String line;
            boolean hasConflict = false;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(": ");
                String uuid = parts[0];
                String[] paths = parts[1].split(" ");
                String sourcePath = paths[0];
                String targetPath = paths[1];
                assertTrue(conflictingUUIDs.contains(uuid));
                assertTrue(conflictingSourcePaths.contains(sourcePath));
                assertTrue(conflictingTargetPaths.contains(targetPath));
                if (!hasConflict) {
                    hasConflict = true;
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
        if (StringUtils.isNotBlank(uuid)) {
            builder.setProperty("jcr:uuid", uuid, Type.STRING);
        }
    }
}