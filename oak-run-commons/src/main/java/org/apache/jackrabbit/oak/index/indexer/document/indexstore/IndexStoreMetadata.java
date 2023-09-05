package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import java.util.Set;
import java.util.function.Predicate;

public class IndexStoreMetadata {

    private String checkpoint;
    private String storeType;
    private String strategy;
    private Set<String> preferredPaths;
    private Predicate<String> pathPredicate;

    public IndexStoreMetadata(String checkpoint, String storeType, String strategy,
                              Set<String> preferredPaths, Predicate<String> pathPredicate) {
        this.checkpoint = checkpoint;
        this.storeType = storeType;
        this.strategy = strategy;
        this.preferredPaths = preferredPaths;
        this.pathPredicate = pathPredicate;
    }

    public IndexStoreMetadata(IndexStoreSortStrategy indexStoreSortStrategy) {
        this.checkpoint = indexStoreSortStrategy.getCheckpoint();
        this.storeType = indexStoreSortStrategy.getStoreType();
        this.strategy = indexStoreSortStrategy.getStrategyName();
        this.preferredPaths = indexStoreSortStrategy.getPreferredPaths();
        this.pathPredicate = indexStoreSortStrategy.getPathPredicate();
    }

    public String getCheckpoint() {
        return checkpoint;
    }

    public String getStoreType() {
        return storeType;
    }

    public String getStrategy() {
        return strategy;
    }

    public Set<String> getPreferredPaths() {
        return preferredPaths;
    }

    public Predicate<String> getPathPredicate() {
        return pathPredicate;
    }
}
