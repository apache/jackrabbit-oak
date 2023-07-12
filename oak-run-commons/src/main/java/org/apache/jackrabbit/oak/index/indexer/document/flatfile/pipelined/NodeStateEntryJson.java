package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.jetbrains.annotations.NotNull;

public class NodeStateEntryJson implements Comparable<NodeStateEntryJson> {
    final String path;
    final String json;

    public NodeStateEntryJson(String path, String json) {
        this.path = path;
        this.json = json;
    }

    @Override
    public int compareTo(@NotNull NodeStateEntryJson o) {
        return this.path.compareTo(o.path);
    }
}
