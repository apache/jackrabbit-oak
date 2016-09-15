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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.io.Closeable;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.commons.concurrent.NotifyingFutureTask;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.lucene.index.IndexableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;

public class DocumentQueue implements Closeable{
    private static final LuceneDoc STOP = LuceneDoc.forUpdate("", "", Collections.<IndexableField>emptyList());
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final IndexTracker tracker;
    private final BlockingQueue<LuceneDoc> docsQueue;
    private final Executor executor;
    private final CounterStats queueSizeStats;

    /**
     * Time in millis for which add call to queue
     * would wait before dropping off
     */
    private final int offerTimeMillis;

    private volatile boolean stopped;

    /**
     * Handler for uncaught exception on the background thread
     */
    private final UncaughtExceptionHandler exceptionHandler = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("Uncaught exception", e);
        }
    };

    /**
     * Current background task
     */
    private volatile NotifyingFutureTask currentTask = NotifyingFutureTask.completed();

    /**
     * Completion handler: set the current task to the next task and schedules that one
     * on the background thread.
     */
    private final Runnable completionHandler = new Runnable() {
        private final Callable<Void> task = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    int maxSize = docsQueue.size();
                    List<LuceneDoc> docs = Lists.newArrayListWithCapacity(maxSize);
                    ListMultimap<String, LuceneDoc> docsPerIndex = ArrayListMultimap.create();

                    //Do the processing in batches
                    int count = docsQueue.drainTo(docs, maxSize);
                    if (count == 0) {
                        return null;
                    }

                    queueSizeStats.dec(count);

                    for (int i = 0; i < count; i++) {
                        LuceneDoc doc = docs.get(i);
                        if (doc == STOP) {
                            return null;
                        }
                        docsPerIndex.get(doc.indexPath).add(doc);
                    }

                    //If required it can optimized by indexing diff indexes in parallel
                    //Something to consider if it becomes a bottleneck
                    for (Map.Entry<String, Collection<LuceneDoc>> e : docsPerIndex.asMap().entrySet()) {
                        processDocs(e.getKey(), e.getValue());
                    }
                    currentTask.onComplete(completionHandler);
                } catch (Throwable t) {
                    exceptionHandler.uncaughtException(Thread.currentThread(), t);
                }
                return null;
            }
        };

        @Override
        public void run() {
            currentTask = new NotifyingFutureTask(task);
            executor.execute(currentTask);
        }
    };

    public DocumentQueue(int maxQueueSize, IndexTracker tracker, Executor executor) {
        this(maxQueueSize, tracker, executor, StatisticsProvider.NOOP);
    }

    public DocumentQueue(int maxQueueSize, IndexTracker tracker, Executor executor, StatisticsProvider sp) {
        this.docsQueue = new LinkedBlockingDeque<>(maxQueueSize);
        this.tracker = tracker;
        this.executor = executor;
        this.offerTimeMillis = 100; //Wait for at most 100 mills while adding stuff to queue
        this.queueSizeStats = sp.getCounterStats("HYBRID_QUEUE_SIZE", StatsOptions.DEFAULT);
    }

    public boolean add(LuceneDoc doc){
        checkState(!stopped);
        boolean added = false;
        try {
            added = docsQueue.offer(doc, offerTimeMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Set the completion handler on the currently running task. Multiple calls
        // to onComplete are not a problem here since we always pass the same value.
        // Thus there is no question as to which of the handlers will effectively run.
        currentTask.onComplete(completionHandler);
        if (added) {
            queueSizeStats.inc();
        }
        return added;
    }

    List<LuceneDoc> getQueuedDocs(){
        List<LuceneDoc> docs = Lists.newArrayList();
        docs.addAll(docsQueue);
        return docs;
    }

    private void processDocs(String indexPath, Iterable<LuceneDoc> docs){

        //Drop the write call if stopped
        if (stopped) {
            return;
        }

        IndexNode indexNode = tracker.acquireIndexNode(indexPath);
        if (indexNode == null) {
            log.debug("No IndexNode found for index [{}].", indexPath);
            return;
        }

        try{
            LuceneIndexWriter writer = indexNode.getLocalWriter();
            for (LuceneDoc doc : docs) {
                if (writer == null) {
                    //IndexDefinition per IndexNode might have changed and local
                    //indexing is disabled. Ignore
                    log.debug("No local IndexWriter found for index [{}]. Skipping index " +
                            "entry for [{}]", indexPath, doc.docPath);
                    return;
                }
                if (doc.delete) {
                    writer.deleteDocuments(doc.docPath);
                } else {
                    writer.updateDocument(doc.docPath, doc.doc);
                }
                log.trace("Updated index with doc {}", doc);
            }
            indexNode.refreshReadersIfRequired();
        } catch (Exception e) {
            //For now we just log it. Later we need to see if frequent error then to
            //temporarily disable indexing for this index
            log.warn("Error occurred while indexing index [{}]",indexPath, e);
        } finally {
            indexNode.release();
        }
    }

    @Override
    public void close() throws IOException {
        //Its fine to "drop" any entry in queue as
        //local index is meant for running state only
        docsQueue.clear();
        docsQueue.add(STOP);
        stopped = true;
    }
}
