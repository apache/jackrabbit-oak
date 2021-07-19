package org.apache.jackrabbit.oak.index.async;

import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

public class AsyncIndexerLucene {


    private NodeStoreFixture fixture;
    private ExtendedIndexHelper extendedIndexHelper;

    public AsyncIndexerLucene(NodeStoreFixture fixture, ExtendedIndexHelper extendedIndexHelper) {
        this.fixture = fixture;
        this.extendedIndexHelper = extendedIndexHelper;
    }

    public void run() {

        String name = "async-service";

        AsyncIndexUpdate task = new AsyncIndexUpdate(name, extendedIndexHelper.getNodeStore(),
                new LuceneIndexEditorProvider(), StatisticsProvider.NOOP, false);

        // Run this task here in a looop after every 25 seconds
    }

}
