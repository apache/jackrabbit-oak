package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;

import java.io.IOException;
import java.util.List;
import java.util.Set;

class TestNodeStateIndexer implements NodeStateIndexer {

    private final String name;
    private final List<String> includedPaths;

    public TestNodeStateIndexer(List<String> includedPaths) {
        this("test-index", includedPaths);
    }

    public TestNodeStateIndexer(String name, List<String> includedPaths) {
        this.name = name;
        this.includedPaths = includedPaths;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean shouldInclude(String path) {
        return includedPaths.stream().anyMatch(path::startsWith);
    }

    @Override
    public boolean shouldInclude(NodeDocument doc) {
        return false;
    }

    @Override
    public boolean index(NodeStateEntry entry) throws IOException, CommitFailedException {
        return false;
    }

    @Override
    public boolean indexesRelativeNodes() {
        return false;
    }

    @Override
    public Set<String> getRelativeIndexedNodeNames() {
        return null;
    }

    @Override
    public String getIndexName() {
        return name;
    }
}