package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class TestRepository {

    private AsyncIndexUpdate asyncIndexUpdate;

    public TestRepository(Oak oak) {
        this.oak = oak;
    }

    public enum NodeStoreType {
        MEMORY_NODE_STORE
    }

    public final int defaultAsyncIndexingTimeInSeconds = 5;

    private boolean isAsync;

    protected NodeStore nodeStore;
    protected Oak oak;

    public Oak getOak() {
        return oak;
    }

    public TestRepository with(boolean isAsync) {
        this.isAsync = isAsync;
        return this;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public TestRepository with(AsyncIndexUpdate asyncIndexUpdate) {
        this.asyncIndexUpdate = asyncIndexUpdate;
        return this;
    }
}
