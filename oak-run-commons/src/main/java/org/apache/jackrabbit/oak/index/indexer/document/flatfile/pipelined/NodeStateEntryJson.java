package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.jetbrains.annotations.NotNull;

public class NodeStateEntryJson implements Comparable<NodeStateEntryJson> {
    final String key;
    final String value;

    public NodeStateEntryJson(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(@NotNull NodeStateEntryJson o) {
        return this.key.compareTo(o.key);
    }
}
