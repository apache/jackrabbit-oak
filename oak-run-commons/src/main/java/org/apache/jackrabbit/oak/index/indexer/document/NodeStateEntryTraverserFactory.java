package org.apache.jackrabbit.oak.index.indexer.document;

public interface NodeStateEntryTraverserFactory {

    NodeStateEntryTraverser create(LastModifiedRange range);

}
