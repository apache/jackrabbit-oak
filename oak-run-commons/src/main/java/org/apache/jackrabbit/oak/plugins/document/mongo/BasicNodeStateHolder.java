package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateHolder;

import java.util.List;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

public class BasicNodeStateHolder implements NodeStateHolder {
    private final List<String> pathElements;
    private final String content;

    public BasicNodeStateHolder(String path, String line) {
        this.content = line;
        ImmutableList.Builder<String> l = new ImmutableList.Builder<>();
        int i = 0;
        for (String part : elements(path)) {
            // This first levels of the path will very likely be similar for most of the entries (e.g. /content/dam/<company>)
            // Interning these strings should provide a big reduction in memory usage.
            // It is not worth to intern all levels because at lower levels the names are more likely to be less diverse,
            // often even unique, so interning them would fill up the interned string hashtable with useless entries.
            if (i < 3) {
                l.add(part.intern());
            } else {
                l.add(part);
            }
            i++;
        }
        this.pathElements = l.build();
    }

    public List<String> getPathElements() {
        return pathElements;
    }

    public String getLine() {
        return content;
    }

    @Override
    public int getMemorySize() {
        return 0;
    }
}