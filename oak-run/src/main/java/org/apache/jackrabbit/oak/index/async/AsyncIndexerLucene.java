package org.apache.jackrabbit.oak.index.async;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.index.IndexCommand;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.run.cli.NodeStoreFixture;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
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
    private final long INIT_DELAY=2;
    private final long delay;
    private final ScheduledExecutorService pool = Executors.newScheduledThreadPool(3);


    public AsyncIndexerLucene(NodeStoreFixture fixture, ExtendedIndexHelper extendedIndexHelper, Closer close, List<String> names, long delay) {
        this.fixture = fixture;
        this.extendedIndexHelper = extendedIndexHelper;
        this.close = close;
        this.names = names;
        this.delay = delay;
    }

    public void run() {
        for(String name : names) {
            log.info("Setting up Async executor for lane - " + name);
            AsyncIndexUpdate task = new AsyncIndexUpdate(name, extendedIndexHelper.getNodeStore(),
                    new LuceneIndexEditorProvider(), StatisticsProvider.NOOP, false);
            close.register(task);
            pool.scheduleWithFixedDelay(task,INIT_DELAY,delay,TimeUnit.SECONDS);
        }
    }

    @Override
    public void close() throws IOException {
        pool.shutdown();
    }
}
