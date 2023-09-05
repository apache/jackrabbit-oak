package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getMetadataFileName;

public abstract class IndexStoreSortStrategyBase implements IndexStoreSortStrategy {
    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * Directory where sorted files will be created.
     */
    protected File storeDir;
    protected Compression algorithm;
    protected Predicate<String> pathPredicate;
    protected Set<String> preferredPaths;
    protected String checkpoint;

    private final String DEFAULT_INDEX_STORE_TYPE = "FlatFileStore";

    public IndexStoreSortStrategyBase(File storeDir, Compression algorithm, Predicate<String> pathPredicate,
                                      Set<String> preferredPaths, String checkpoint) {
        this.storeDir = storeDir;
        this.algorithm = algorithm;
        this.pathPredicate = pathPredicate;
        this.preferredPaths = preferredPaths;
        this.checkpoint = checkpoint;
    }

    @Override
    public String getStrategyName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getStoreType() {
        return DEFAULT_INDEX_STORE_TYPE;
    }

    @Override
    public String getCheckpoint() {
        return checkpoint;
    }

    @Override
    public Set<String> getPreferredPaths() {
        return preferredPaths;
    }

    @Override
    public Predicate<String> getPathPredicate() {
        return pathPredicate;
    }

    @Override
    public File createMetadataFile() throws IOException {
        File metadataFile = new File(storeDir, getMetadataFileName(algorithm));
        IndexStoreMetadata indexStoreMetadata = new IndexStoreMetadata(this);
        try (BufferedWriter metadataWriter = FlatFileStoreUtils.createWriter(metadataFile, algorithm)) {
            writeMetadataToFile(metadataWriter, indexStoreMetadata);
        }
        log.info("Created metadataFile:{} with strategy:{} ", metadataFile.getPath(), indexStoreMetadata.getStoreType());
        return metadataFile;
    }

    private void writeMetadataToFile(BufferedWriter w, IndexStoreMetadata indexStoreMetadata) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(w, indexStoreMetadata);
    }
}
