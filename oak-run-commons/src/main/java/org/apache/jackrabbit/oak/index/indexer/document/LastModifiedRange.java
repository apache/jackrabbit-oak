package org.apache.jackrabbit.oak.index.indexer.document;

public class LastModifiedRange {

    private final long lastModifiedLowerBound;
    private final long lastModifiedUpperBound;

    public LastModifiedRange(long lastModifiedLowerBound, long lastModifiedUpperBound) {
        this.lastModifiedLowerBound = lastModifiedLowerBound;
        this.lastModifiedUpperBound = lastModifiedUpperBound;
    }

    public long getLastModifiedLowerBound() {
        return lastModifiedLowerBound;
    }

    public long getLastModifiedUpperBound() {
        return lastModifiedUpperBound;
    }
}
