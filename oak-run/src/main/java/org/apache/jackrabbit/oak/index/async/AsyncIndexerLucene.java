package org.apache.jackrabbit.oak.index.async;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.index.IndexCommand;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AsyncIndexerLucene implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AsyncIndexerLucene.class);
    private final NodeStoreFixture fixture;
    private final ExtendedIndexHelper extendedIndexHelper;
    private final Closer close;
    private final List<String> names;
    private final long INIT_DELAY=0;
    private final long delay;
    private final ScheduledExecutorService pool;


    public AsyncIndexerLucene(NodeStoreFixture fixture, ExtendedIndexHelper extendedIndexHelper, Closer close, List<String> names, long delay) {
        this.fixture = fixture;
        this.extendedIndexHelper = extendedIndexHelper;
        this.close = close;
        this.names = names;
        this.delay = delay;
        pool = Executors.newScheduledThreadPool(names.size());
    }

    public void run() {

        for(String name : names) {
            log.info("Setting up Async executor for lane - " + name);

            IndexEditorProvider composite = CompositeIndexEditorProvider
                    .compose(Arrays.asList(new LuceneIndexEditorProvider(), new NodeCounterEditorProvider()));
            AsyncIndexUpdate task = new AsyncIndexUpdate(name, extendedIndexHelper.getNodeStore(),
                    composite, StatisticsProvider.NOOP, false);
            // TODO : Handle closure for AsyncIndexUpdate - during command exit, problem is when to do it ? We want this to run in infinite loop
            // TODO : In oak, it gets closed with system bundle deactivation
            //close.register(task);
            pool.scheduleWithFixedDelay(task,INIT_DELAY,delay,TimeUnit.SECONDS);
        }
    }

    @Override
    public void close() throws IOException {
        pool.shutdown();
    }
}
