package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import java.util.List;
import java.util.function.Predicate;

public class IncrementalIndexStoreMetadata {

    private String beforeCheckpoint;
    private String afterCheckpoint;
    private String storeType;
    private String strategy;

    private List<String> preferredPaths;
    private Predicate<String> pathPredicate;

    public IncrementalIndexStoreMetadata(IncrementalIndexStoreSortStrategy indexStoreSortStrategy) {
        this.beforeCheckpoint = indexStoreSortStrategy.getBeforeCheckpoint();
        this.afterCheckpoint = indexStoreSortStrategy.getAfterCheckpoint();
        this.storeType = indexStoreSortStrategy.getStoreType();
        this.strategy = indexStoreSortStrategy.getStrategyName();
        this.preferredPaths = indexStoreSortStrategy.getPreferredPaths();
        this.pathPredicate = indexStoreSortStrategy.getPathPredicate();
    }

    public String getBeforeCheckpoint() {
        return beforeCheckpoint;
    }

    public String getAfterCheckpoint() {
        return afterCheckpoint;
    }

    public String getStrategy() {
        return strategy;
    }

    public String getStoreType() {
        return storeType;
    }


    public List<String> getPreferredPaths() {
        return preferredPaths;
    }

    public Predicate<String> getPathPredicate() {
        return pathPredicate;
    }
}
