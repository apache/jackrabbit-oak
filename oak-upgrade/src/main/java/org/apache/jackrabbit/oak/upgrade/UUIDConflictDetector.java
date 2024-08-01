package org.apache.jackrabbit.oak.upgrade;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UUIDConflictDetector {

    private static final Logger log = LoggerFactory.getLogger(UUIDConflictDetector.class);
    private final File dir = new File("/tmp");
    private final NodeStore sourceStore;
    private final NodeStore targetStore;

    public UUIDConflictDetector(NodeStore sourceStore, NodeStore targetStore) {
        this.sourceStore = sourceStore;
        this.targetStore = targetStore;
    }

    private NodeState getNodeAtPath(NodeState node, String path) {
        for (String name : path.substring(1).split("/")) {
            node = node.getChildNode(name);
        }
        return node;
    }


    public void detectConflicts() throws IOException {
        File sourceFile = gatherUUIDs(sourceStore.getRoot(), "source");
        File targetFile = gatherUUIDs(targetStore.getRoot(), "target");

        compareUUIDs(sourceFile, targetFile);
    }

    public void detectConflicts(String[] includePath) throws IOException {
        Set<String> includePaths = dedupePaths(includePath);
        if (CollectionUtils.isEmpty(includePaths)) {
            log.info("Include paths not provided, iterating entire repository to detect conflicts");
            detectConflicts();
            return;
        }

        File sourceFileForPaths = File.createTempFile("source_uuids_" + Instant.now().toEpochMilli(), ".txt", dir);
        try (BufferedWriter writer = Files.newBufferedWriter(sourceFileForPaths.toPath())) {
            for (String path : includePaths) {
                NodeState state = getNodeAtPath(sourceStore.getRoot(), path);
                gatherUUIDs(state, path, writer);
            }
        }

        File sourceFile = sortFile(sourceFileForPaths);
        File targetFile = gatherUUIDs(targetStore.getRoot(), "target");

        compareUUIDs(sourceFile, targetFile);

    }

    private Set<String> dedupePaths(String[] includePaths) {
        if (includePaths == null || includePaths.length == 0) {
            return Collections.emptySet();
        }

        Set<String> uniqueIncludePaths = Arrays.stream(includePaths).filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
        Set<String> dedupePaths = new HashSet<>();

        // remove child path if parent path is parent path is present
        for (String currentPath : uniqueIncludePaths) {
            String parentPath = currentPath.substring(0, currentPath.lastIndexOf('/'));
            if (uniqueIncludePaths.contains(parentPath)) {
                dedupePaths.add(parentPath);
            } else {
                dedupePaths.add(currentPath);
            }
        }

        return dedupePaths;
    }


    private File gatherUUIDs(NodeState state, String prefix) throws IOException {
        File file = File.createTempFile(prefix + "_uuids_" + Instant.now().toEpochMilli(), ".txt", dir);
        try (BufferedWriter writer = Files.newBufferedWriter(file.toPath())) {
            gatherUUIDs(state, "", writer);
        }
        return sortFile(file);
    }

    private File sortFile(File file) throws IOException {
        List<File> sortedFiles = ExternalSort.sortInBatch(file, Comparator.naturalOrder());
        return sortedFiles.get(0);
    }

    private void gatherUUIDs(NodeState state, String path, BufferedWriter writer) throws IOException {
        if (state.hasProperty("jcr:uuid")) {
            String uuid = state.getString("jcr:uuid");
            writer.write(uuid + " -> " + (StringUtils.isBlank(path) ? "/" : path));
            writer.newLine();
        }

        for (ChildNodeEntry child : state.getChildNodeEntries()) {
            gatherUUIDs(child.getNodeState(), path + "/" + child.getName(), writer);
        }
    }

    private void compareUUIDs(File sourceFile, File targetFile) throws IOException {
        try (BufferedReader sourceReader = Files.newBufferedReader(sourceFile.toPath());
             BufferedReader targetReader = Files.newBufferedReader(targetFile.toPath());
             BufferedWriter conflictWriter = Files.newBufferedWriter(Paths.get("/tmp/uuid_conflicts_" + System.currentTimeMillis() + ".txt"))) {

            String sourceLine = sourceReader.readLine();
            String targetLine = targetReader.readLine();

            while (sourceLine != null && targetLine != null) {
                String[] sourceLineSplit = sourceLine.split(" -> ");
                String[] targetLineSplit = targetLine.split(" -> ");
                String sourceUUID = sourceLineSplit[0];
                String sourcePath = sourceLineSplit[1];
                String targetUUID = targetLineSplit[0];
                String targetPath = targetLineSplit[1];

                int comparison = sourceUUID.compareTo(targetUUID);
                if (comparison < 0) {
                    sourceLine = sourceReader.readLine();
                } else if (comparison > 0) {
                    targetLine = targetReader.readLine();
                } else {
                    if (!StringUtils.equals(sourcePath, targetPath)) {
                        String uuidWithPaths = sourceUUID + ": " + sourcePath + " " + targetPath;
                        conflictWriter.write(uuidWithPaths);
                        conflictWriter.newLine();
                    }
                    sourceLine = sourceReader.readLine();
                    targetLine = targetReader.readLine();
                }
            }
        }
    }
}
