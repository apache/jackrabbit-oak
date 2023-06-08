package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class NodeStateEntryBatch {
    private final ByteBuffer buffer;
    private final ArrayList<SortKey> sortBuffer;
    private final int maxEntries;

    public NodeStateEntryBatch(ByteBuffer buffer, int maxEntries) {
        this.buffer = buffer;
        this.maxEntries = maxEntries;
        this.sortBuffer = new ArrayList<>(maxEntries);
    }

    public ArrayList<SortKey> getSortBuffer() {
        return sortBuffer;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public boolean isAtMaxEntries() {
        if (sortBuffer.size() > maxEntries) {
            throw new AssertionError("Sort buffer size exceeded max entries: " + sortBuffer.size() + " > " + maxEntries);
        }
        return sortBuffer.size() == maxEntries;
    }

    public void reset() {
        buffer.clear();
        sortBuffer.clear();
    }

    public static NodeStateEntryBatch createNodeStateEntryBatch(int bufferSize, int maxNumEntries) {
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        return new NodeStateEntryBatch(buffer, maxNumEntries);
    }
}