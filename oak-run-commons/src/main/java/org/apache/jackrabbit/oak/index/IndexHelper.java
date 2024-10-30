/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexInfoServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathServiceImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexPrinter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class IndexHelper implements Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    protected final NodeStore store;
    protected final File outputDir;
    protected final File workDir;
    private final IndexingReporter indexReporter;
    private IndexPathService indexPathService;
    private AsyncIndexInfoService asyncIndexInfoService;
    protected final List<String> indexPaths;
    private final Whiteboard whiteboard;
    private Executor executor;
    protected final Closer closer = Closer.create();
    private final BlobStore blobStore;
    private final StatisticsProvider statisticsProvider;
    private IndexInfoServiceImpl indexInfoService;


    protected IndexHelper(NodeStore store, BlobStore blobStore, Whiteboard whiteboard,
                File outputDir, File workDir, List<String> indexPaths) {
        this.store = store;
        this.blobStore = blobStore;
        this.whiteboard = whiteboard;
        this.statisticsProvider = requireNonNull(WhiteboardUtils.getService(whiteboard, StatisticsProvider.class));
        this.indexReporter = requireNonNull(WhiteboardUtils.getService(whiteboard, IndexingReporter.class));
        this.outputDir = outputDir;
        this.workDir = workDir;
        this.indexPaths = List.copyOf(indexPaths);
    }

    public NodeStore getNodeStore() {
        return store;
    }

    public File getOutputDir() {
        return outputDir;
    }

    public File getWorkDir() {
        return workDir;
    }

    public IndexPrinter getIndexPrinter() {
        return new IndexPrinter(getIndexInfoService(), getAsyncIndexInfoService());
    }

    public IndexDefinitionPrinter getIndexDefnPrinter() {
        return new IndexDefinitionPrinter(store, getIndexPathService());
    }

    public IndexPathService getIndexPathService() {
        if (indexPathService == null) {
            if (indexPaths.isEmpty()) {
                indexPathService = new IndexPathServiceImpl(store);
            } else {
                indexPathService = () -> indexPaths;
            }
        }
        return indexPathService;
    }

    public List<String> getIndexPaths() {
        return indexPaths;
    }

    public Executor getExecutor() {
        if (executor == null) {
            ExecutorService executorService = createExecutor();
            closer.register(new ExecutorCloser(executorService));
            executor = executorService;
        }
        return executor;
    }

    public MountInfoProvider getMountInfoProvider() {
        return Mounts.defaultMountInfoProvider();
    }

    public StatisticsProvider getStatisticsProvider() {
        return statisticsProvider;
    }

    @Nullable
    public GarbageCollectableBlobStore getGCBlobStore() {
        return blobStore instanceof GarbageCollectableBlobStore ? (GarbageCollectableBlobStore) blobStore : null;
    }

    @Nullable
    public <T> T getService(@NotNull Class<T> type) {
        return WhiteboardUtils.getService(whiteboard, type);
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    protected AsyncIndexInfoService getAsyncIndexInfoService() {
        if (asyncIndexInfoService == null) {
            asyncIndexInfoService = new AsyncIndexInfoServiceImpl(store);
        }
        return asyncIndexInfoService;
    }

    private IndexInfoService getIndexInfoService() {
        if (indexInfoService == null) {
            indexInfoService = new IndexInfoServiceImpl(store, getIndexPathService());
            bindIndexInfoProviders(indexInfoService);
        }
        return indexInfoService;
    }

    protected void bindIndexInfoProviders(IndexInfoServiceImpl indexInfoService) {
        // Override this if needed in Extended IndexHelper
        // Only applicable for Lucene and Property Index as of now
    }

    private ThreadPoolExecutor createExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 5, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            private final Thread.UncaughtExceptionHandler handler =
                    (t, e) -> log.warn("Error occurred in asynchronous processing ", e);

            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread thread = new Thread(r, createName());
                thread.setDaemon(true);
                thread.setPriority(Thread.MIN_PRIORITY);
                thread.setUncaughtExceptionHandler(handler);
                return thread;
            }

            private String createName() {
                return "oak-lucene-" + counter.getAndIncrement();
            }
        });
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    public IndexingReporter getIndexReporter() {
        return indexReporter;
    }
}
