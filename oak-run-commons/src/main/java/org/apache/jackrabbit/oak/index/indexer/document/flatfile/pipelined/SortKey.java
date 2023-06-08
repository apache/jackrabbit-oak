package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

public final class SortKey {
    private final String[] pathElements;
    private final int bufferPos;

    public SortKey(String[] pathElements, int bufferPos) {
        this.pathElements = pathElements;
        this.bufferPos = bufferPos;
    }

    public int getBufferPos() {
        return bufferPos;
    }

    public String[] getPathElements() {
        return pathElements;
    }

}
