package org.apache.jackrabbit.oak.indexversion;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

public class LucenePurgeOldIndexVersion extends PurgeOldIndexVersion {
    @Override
    protected String getIndexType() {
        return TYPE_LUCENE;
    }

    @Override
    protected void postDeleteOp(String idxPath) {
     // NOOP
    }

    @Override
    protected boolean returnIgnoreIsIndexActiveCheck() {
        return false;
    }

    @Override
    protected void preserveDetailsFromIndexDefForPostOp(NodeBuilder builder) {
        // NOOP
    }
}
