package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

public interface IncrementalIndexStoreSortStrategy extends SortStrategy {

    String getStrategyName();

    String getBeforeCheckpoint();

    String getAfterCheckpoint();

    List<String> getPreferredPaths();

    Predicate<String> getPathPredicate();

    File createMetadataFile() throws IOException;

    String getStoreType();
}
