package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ConflictDetectorMain {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: java -jar script.jar <sourceRepoPath> <targetRepoPath> <subtreePath>");
            System.exit(1);
        }

        String sourceRepoPath = args[0];
        String targetRepoPath = args[1];
        String subtreePath = args[2];
        Closer closer = Closer.create();
        try {
//            FileStore sourceFileStore = FileStoreBuilder.fileStoreBuilder(new File(sourceRepoPath)).build();
//            FileStore targetStore = FileStoreBuilder.fileStoreBuilder(new File(targetRepoPath)).build();

            ReadOnlyFileStore sourceFileStore = FileStoreBuilder.fileStoreBuilder(new File(sourceRepoPath)).buildReadOnly();
            ReadOnlyFileStore targetStore = FileStoreBuilder.fileStoreBuilder(new File(targetRepoPath)).buildReadOnly();

            closer.register(FileStoreUtils.asCloseable(sourceFileStore));
            closer.register(FileStoreUtils.asCloseable(targetStore));

            NodeStore sourceNodeStore = SegmentNodeStoreBuilders.builder(sourceFileStore).build();
            NodeStore targetNodeStore = SegmentNodeStoreBuilders.builder(targetStore).build();


            UUIDConflictDetector detector = new UUIDConflictDetector(sourceNodeStore, targetNodeStore);
            Map<String, Map.Entry<String, String>> conflicts = detector.detectConflicts(subtreePath);

            for (Map.Entry<String, Map.Entry<String, String>> entry : conflicts.entrySet()) {
                String uuid = entry.getKey();
                String sourcePath = entry.getValue().getKey();
                String targetPath = entry.getValue().getValue();
                System.out.println("Conflict detected for UUID: " + uuid + " at source path: " + sourcePath + " and target path: " + targetPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            closer.close();
        }
    }
}
