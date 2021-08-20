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
package org.apache.jackrabbit.oak.index.async;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AsyncIndexerBase implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AsyncIndexerBase.class);
    private final IndexHelper indexHelper;
    protected final Closer closer;
    private final List<String> names;
    private static final long INIT_DELAY = 0;
    private final long delay;
    private final ScheduledExecutorService pool;
    private final CountDownLatch latch;

    public AsyncIndexerBase(IndexHelper indexHelper, Closer closer, List<String> names, long delay) {
        this.indexHelper = indexHelper;
        this.closer = closer;
        this.names = names;
        this.delay = delay;
        pool = Executors.newScheduledThreadPool(names.size());
        latch = new CountDownLatch(1);
    }

    public void execute() throws InterruptedException, IOException {
        addShutDownHook();
        IndexEditorProvider editorProvider = getIndexEditorProvider();
        // This can be null in case of any exception while initializing index copier in lucene.
        if (editorProvider == null) {
            log.error("EditorProvider is null, can't proceed further. Exiting");
            closer.close();
        }
        // Register async tasks for all lanes to the ScheduledThreadPoolExecutor
        for (String name : names) {
            log.info("Setting up Async executor for lane - " + name);
            AsyncIndexUpdate task = new AsyncIndexUpdate(name, indexHelper.getNodeStore(),
                    editorProvider, StatisticsProvider.NOOP, false);
            closer.register(task);

            pool.scheduleWithFixedDelay(task, INIT_DELAY, delay, TimeUnit.SECONDS);
        }
        // Make the main thread wait now, since we want this to run continuously
        // Although ScheduledExecutorService would still keep executing even if we let the main thread exit
        // but it will cleanup logging resources and other closeables and create problems.
        latch.await();
    }

    @Override
    public void close() throws IOException {
        log.info("Closing down Async Indexer Service...");
        latch.countDown();
        pool.shutdown();
    }

    /**
    Since this would be running continuously in a loop, we can't possibly call closures in a normal conventional manner
    otherwise resources would be closed from the main thread and spawned off threads will still be running and will fail.
    So we handle closures as part of shut down hooks in case of SIGINT, SIGTERM etc.
     **/
    private void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    closer.close();
                } catch (IOException e) {
                    log.error("Exception during cleanup ", e);
                }
            }
        });
    }

    public abstract IndexEditorProvider getIndexEditorProvider();

}



