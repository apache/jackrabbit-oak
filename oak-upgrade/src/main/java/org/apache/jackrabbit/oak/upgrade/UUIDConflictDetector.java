package org.apache.jackrabbit.oak.upgrade;

import ai.djl.ModelException;
import ai.djl.translate.TranslateException;
import java.io.InputStream;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;

public class UUIDConflictDetector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(UUIDConflictDetector.class);
    private final File dir;
    private final NodeStore sourceStore;
    private final NodeStore targetStore;
    private long timeStamp;
    private final List<String> deleteFilePaths;

    public UUIDConflictDetector(NodeStore sourceStore, NodeStore targetStore, File dir) {
        this.sourceStore = sourceStore;
        this.targetStore = targetStore;
        this.dir = dir;
        deleteFilePaths = new ArrayList<>();
        this.timeStamp = 0;
    }

    // for testing purposes only, not for production usage
    public UUIDConflictDetector(NodeStore sourceStore, NodeStore targetStore, File dir, long timeStamp) {
        this(sourceStore, targetStore, dir);
        this.timeStamp = timeStamp;
    }


    private void detectConflicts() throws IOException {
        File sourceFile = gatherUUIDs(sourceStore.getRoot(), "source");
        File targetFile = gatherUUIDs(targetStore.getRoot(), "target");

        compareUUIDs(sourceFile, targetFile);
    }

    public void detectConflicts(String[] includePath) throws IOException {
        long startTime = System.currentTimeMillis();
        log.info("started detecting uuid conflicts at: {}", startTime);
        Set<String> includePaths = dedupePaths(includePath);
        if (CollectionUtils.isEmpty(includePaths)) {
            log.info("include paths not provided, iterating entire repository to detect conflicts");
            detectConflicts();
            log.info("uuid conflict detection completed in: {} ms", System.currentTimeMillis() - startTime);
            return;
        }

        File sourceFile = getSourceFileForPaths(includePaths);
        File targetFile = gatherUUIDs(targetStore.getRoot(), "target");

        compareUUIDs(sourceFile, targetFile);
        log.info("uuid conflict detection completed in: {} ms", System.currentTimeMillis() - startTime);
    }

    private File getSourceFileForPaths(Set<String> includePaths) throws IOException {
        long startTime = System.currentTimeMillis();
        log.info("starting fetching uuid nodes from source repository at: {}", startTime);
        File sourceFile = new File(dir, "source_uuids_" + getTimeStamp() + ".txt");
        log.info("source file: {}", sourceFile.getName());
        deleteFilePaths.add(sourceFile.getAbsolutePath());
        try (BufferedWriter writer = Files.newBufferedWriter(sourceFile.toPath())) {
            for (String path : includePaths) {
                NodeState state = getNodeAtPath(sourceStore.getRoot(), path);
                gatherUUIDs(state, path, writer);
            }
        }
        log.info("fetching uuid nodes completed from source repository in: {} ms", System.currentTimeMillis() - startTime);
        return sortFile(sourceFile, "source");
    }

    private NodeState getNodeAtPath(NodeState node, String path) {
        for (String name : path.substring(1).split("/")) {
            node = node.getChildNode(name);
        }
        return node;
    }

    /* remove the child paths from includePaths if parent path is provided.
     * For example,
     * includePaths = ["/content/foo/a, /content/foo/b, /content/foo, /content/bar"] will be reduced to
     * ["/content/foo, /content/bar"]
     * */
    private Set<String> dedupePaths(String[] includePaths) {
        if (includePaths == null || includePaths.length == 0) {
            return Collections.emptySet();
        }

        Set<String> uniqueIncludePaths = Arrays.stream(includePaths).filter(StringUtils::isNotBlank)
          .collect(Collectors.toSet());
        Set<String> dedupePaths = new HashSet<>();

        // remove child path if parent path is present
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
        long startTime = System.currentTimeMillis();
        log.info("starting fetching uuid nodes from {} repository at: {}", prefix, startTime);
        File file = new File(dir, prefix + "_uuids_" + getTimeStamp() + ".txt");
        log.info("{} uuid file: {}", prefix, file.getName());
        deleteFilePaths.add(file.getAbsolutePath());
        try (BufferedWriter writer = Files.newBufferedWriter(file.toPath())) {
            gatherUUIDs(state, "", writer);
        }
        log.info("fetching uuid nodes completed from {} repository in: {} ms", prefix, System.currentTimeMillis() - startTime);
        return sortFile(file, prefix);
    }

    private File sortFile(File file, String prefix) throws IOException {
        long startTime = System.currentTimeMillis();
        log.info("sorting {} started at: {}", file.getName(), startTime);
        List<File> sortedFiles = ExternalSort.sortInBatch(file, Comparator.naturalOrder());
        log.info("sorting {} file completed in: {} ms", file.getName(), System.currentTimeMillis() - startTime);
        File sortedFile = new File(dir, prefix + "_uuids_" + getTimeStamp() + ".txt");
        deleteFilePaths.add(sortedFile.getAbsolutePath());
        log.info("sorted {} uuid file: {}", prefix, sortedFile.getName());

        long mergeFileStartTime = System.currentTimeMillis();
        log.info("merging sorted {} files started at: {} ms", prefix, mergeFileStartTime);
        // Merge the sorted temporary files into the sortedFile
        ExternalSort.mergeSortedFiles(sortedFiles, sortedFile);

        log.info("merging sorted {} files completed in: {} ms", prefix, System.currentTimeMillis() - mergeFileStartTime);
        return sortedFile;
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
        long startTime = System.currentTimeMillis();
        log.info("started uuid conflict comparison in {}, {} files at: {}", sourceFile.getName(), targetFile.getName(), startTime);
        Path uuidConflictFilePath = Paths.get(dir.getAbsolutePath(), "uuid_conflicts_" + getTimeStamp() + ".txt");
        log.info("uuid conflict file: {}", uuidConflictFilePath.getFileName());
        try (BufferedReader sourceReader = Files.newBufferedReader(sourceFile.toPath());
          BufferedReader targetReader = Files.newBufferedReader(targetFile.toPath());
          BufferedWriter conflictWriter = Files.newBufferedWriter(uuidConflictFilePath)) {

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
                        log.info("conflict found for uuid: {}, source path: {}, target path: {}", sourceUUID, sourcePath, targetPath);
                        String uuidWithPaths = sourceUUID + ": " + sourcePath + " " + targetPath;
                        conflictWriter.write(uuidWithPaths);
                        conflictWriter.newLine();
                        resolveConflict(sourcePath, targetPath);
                    }
                    sourceLine = sourceReader.readLine();
                    targetLine = targetReader.readLine();
                }
            }
        }
        log.info("uuid conflict comparison in {}, {} files completed in: {} ms", sourceFile.getName(), targetFile.getName(), System.currentTimeMillis() - startTime);
    }

    private boolean isImage(NodeState node) {
        NodeState metadataNode = node.getChildNode("jcr:content").getChildNode("metadata");
        PropertyState mimeTypeProperty = metadataNode.getProperty("dam:MIMEtype");
        return mimeTypeProperty != null && "image/png".equals(mimeTypeProperty.getValue(Type.STRING));
    }

    private InputStream getBinaryContent(NodeState node) throws IOException {
        log.info("getting binary content for node: {}", node);
        NodeState contentNode = node.getChildNode("jcr:content");
        log.info("content node: {}", contentNode);
        NodeState rendition = contentNode.getChildNode("renditions");
        log.info("rendition: {}", rendition);
        NodeState originalRendition = rendition.getChildNode("original");
        log.info("original rendition: {}", originalRendition);
        NodeState originalRenditionChild = originalRendition.getChildNode("jcr:content");
        log.info("original rendition child: {}", originalRenditionChild);

        PropertyState binaryState = originalRenditionChild.getProperty("jcr:data");
        log.info("binary state: {}", binaryState);
        if (binaryState != null && binaryState.getType() == Type.BINARY) {
            Object binaryValue = binaryState.getValue(Type.BINARY);
            if (binaryValue instanceof SegmentBlob) {
                SegmentBlob segmentBlob = (SegmentBlob) binaryValue;
                log.info("segment blob: {}", segmentBlob);
                return segmentBlob.getNewStream();
            } else {
                log.error("binary value is not an instance of SegmentBlob: {}", binaryValue.getClass());
            }
        }
        log.info("unable to retrieve binary content");
        return null;
    }
    private void resolveConflict(String sourcePath, String targetPath) throws IOException {
        boolean isMetadataMatch = compareMetadata(sourcePath, targetPath);
        log.info("metadata match for source path: {}, target path: {} isMetadataMatch: {}", sourcePath, targetPath, isMetadataMatch);
        if (isMetadataMatch) {
            // proceed with Binary Comparison
            NodeState sourceNode = getNodeAtPath(sourceStore.getRoot(), sourcePath);
            NodeState targetNode = getNodeAtPath(targetStore.getRoot(), targetPath);

            if (isImage(sourceNode) && isImage(targetNode)) {
                try (InputStream sourceStream = getBinaryContent(sourceNode);
                  InputStream targetStream = getBinaryContent(targetNode)) {
                    log.info("source stream: {}, target stream: {}", sourceStream, targetStream);
                    if (sourceStream != null && targetStream != null) {
                        // Send to image comparison service/
                        File tmpDir = new File("/tmp/oakconflict");
                        if (!tmpDir.exists()) {
                            tmpDir.mkdirs();
                        }
                        File sourceFile = new File(tmpDir, "source_image.png");
                        try (java.io.OutputStream outStream = new java.io.FileOutputStream(sourceFile)) {
                            byte[] buffer = new byte[1024];
                            int bytesRead;
                            while ((bytesRead = sourceStream.read(buffer)) != -1) {
                                outStream.write(buffer, 0, bytesRead);
                            }
                        }
                        File targetFile = new File(tmpDir, "target_image.png");
                        try (java.io.OutputStream outStream = new java.io.FileOutputStream(targetFile)) {
                            byte[] buffer = new byte[1024];
                            int bytesRead;
                            while ((bytesRead = targetStream.read(buffer)) != -1) {
                                outStream.write(buffer, 0, bytesRead);
                            }
                        }
                        ImageEmbeddingComparison.compareImages(sourceFile.getAbsolutePath(), targetFile.getAbsolutePath());
                        log.info("Image comparison completed for source and target images");
                    } else {
                        log.warn("Failed to fetch InputStream for source or target image. SourceStream: {}, TargetStream: {}", sourceStream, targetStream);
                    }
                } catch (Exception e) {
                    log.warn(e.getMessage());
                    log.warn("Failed to fetch InputStream for source or target image. SourceStream");
                }
            }

        } else {
            log.info("metadata mismatch for source path: {}, target path: {}", sourcePath, targetPath);
        }
    }

    private boolean compareMetadata(String sourcePath, String targetPath) {
        log.info("comparing metadata for source path: {}, target path: {}", sourcePath, targetPath);
        Map<String, Object> sourceMetadata = fetchMetadata(sourcePath, sourceStore);
        Map<String, Object> targetMetadata = fetchMetadata(targetPath, targetStore);
        targetMetadata.putAll(fetchMetadata(targetPath + "/jcr:content/metadata", targetStore));
        sourceMetadata.putAll(fetchMetadata(sourcePath + "/jcr:content/metadata", sourceStore));
        List<String> properties = Arrays.asList("jcr:primaryType", "jcr:mixinTypes", "dam:size", "dam:MIMEtype", "dam:Fileformat");
        return properties.stream().allMatch(property -> {
            Object sourceValue = sourceMetadata.get(property);
            Object targetValue = targetMetadata.get(property);
            if (sourceValue instanceof PropertyState && targetValue instanceof PropertyState) {
                return compareProperties((PropertyState) sourceValue, (PropertyState) targetValue);
            } else {
                return sourceValue != null && sourceValue.equals(targetValue);
            }
        });
    }

    private boolean compareProperties(PropertyState property1, PropertyState property2) {
        if (property1 == null || property2 == null) {
            return false;
        }
        if (!property1.getType().equals(property2.getType())) {
            return false;
        }
        return property1.getValue(property1.getType()).equals(property2.getValue(property2.getType()));
    }

    private Map<String, Object> fetchMetadata(String path, NodeStore nodeStore) {
        Map<String, Object> metadata = new HashMap<>();
        NodeState root = nodeStore.getRoot();
        NodeState node = getNodeAtPath(root, path);
        if (node == null) {
            return metadata;
        }
        node.getProperties().forEach(property -> {
            try {
                metadata.put(property.getName(), property.getValue(property.getType()));
            } catch (Exception e) {
                log.error("Error while fetching metadata for property: {}", property.getName(), e);
            }
        });
        return metadata;
    }

    public long getTimeStamp() {
        return timeStamp == 0L ? Instant.now().toEpochMilli() : timeStamp;
    }

    @Override
    public void close() {
        for (String deleteFilePath : deleteFilePaths) {
            File fileToDelete = new File(deleteFilePath);
            if (fileToDelete.exists()) {
                if (fileToDelete.delete()) {
                    log.info("deleted file: {}", fileToDelete.getAbsolutePath());
                } else {
                    log.info("unable to delete file: {}", fileToDelete.getAbsolutePath());
                }
            }
        }
    }
}
