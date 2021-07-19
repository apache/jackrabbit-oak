package org.apache.jackrabbit.oak.index.async;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

public class AsyncIndexerLucene {


    private NodeStoreFixture fixture;
    private ExtendedIndexHelper extendedIndexHelper;
    private Closer close;

    public AsyncIndexerLucene(NodeStoreFixture fixture, ExtendedIndexHelper extendedIndexHelper, Closer close) {
        this.fixture = fixture;
        this.extendedIndexHelper = extendedIndexHelper;
        this.close = close;
    }

    public void run() {

        String name = "service-async";

        AsyncIndexUpdate task = new AsyncIndexUpdate(name, extendedIndexHelper.getNodeStore(),
                new LuceneIndexEditorProvider(), StatisticsProvider.NOOP, false);

        close.register(task);
        // Run this task here in a looop after every 25 seconds

        task.run();
    }

}
