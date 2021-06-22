package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;

public interface TraverseAndSortTaskFactory {

    TraverseAndSortTask createTask(NodeStateEntryTraverser nodeStateEntryTraverser);

}
