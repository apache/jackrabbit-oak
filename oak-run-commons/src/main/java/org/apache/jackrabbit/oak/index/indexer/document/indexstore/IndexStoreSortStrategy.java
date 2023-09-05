package org.apache.jackrabbit.oak.index.indexer.document.indexstore;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

public interface IndexStoreSortStrategy extends SortStrategy {

    String getStrategyName();

    String getStoreType();

    String getCheckpoint();

    Set<String> getPreferredPaths();

    Predicate<String> getPathPredicate();

    File createMetadataFile() throws IOException;

}
