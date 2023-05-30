package org.apache.jackrabbit.oak.plugins.document.mongo;

public class TransformResult {
    private final long entryCount;

    public TransformResult(long entryCount) {
        this.entryCount = entryCount;
    }

    public long getEntryCount() {
        return entryCount;
    }

}