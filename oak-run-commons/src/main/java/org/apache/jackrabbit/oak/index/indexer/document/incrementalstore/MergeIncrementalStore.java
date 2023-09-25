package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import java.io.IOException;

public interface MergeIncrementalStore {
    void doMerge() throws IOException;

    String getStrategyName();
}
