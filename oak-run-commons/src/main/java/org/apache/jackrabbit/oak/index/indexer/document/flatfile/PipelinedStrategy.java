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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.BasicDBObject;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.BasicNodeStateHolder;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.plugins.document.mongo.PipelineMongoDownloadTask;
import org.apache.jackrabbit.oak.plugins.document.mongo.PipelinedSortBatchTask;
import org.apache.jackrabbit.oak.plugins.document.mongo.PipelinedTransformTask;
import org.apache.jackrabbit.oak.plugins.document.mongo.TransformResult;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.NotificationEmitter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.jackrabbit.guava.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;

public class PipelinedStrategy implements SortStrategy {
    public static final BasicDBObject[] SENTINEL_MONGO_DOCUMENT = new BasicDBObject[0];
    public static final ArrayList<BasicNodeStateHolder> SENTINEL_ENTRY_BATCH = new ArrayList<>(0);

    public static final String MONGO_DOC_QUEUE_SIZE = "MONGO_DOC_QUEUE_SIZE";
    private static final String MONGO_BLOCK_SIZE = "MONGO_DOC_BATCH_SIZE";
    public static final String PIPELINED_NSE_BATCH_SIZE = "PIPELINED_NSE_BATCH_SIZE";
    private static final String PIPELINED_TRANSFORM_THREADS = "PIPELINED_TRANSFORM_THREADS";
    private static final String PIPELINED_SORT_BUFFER_SIZE = "PIPELINED_SORT_BUFFER_SIZE";


    private final Logger log = LoggerFactory.getLogger(getClass());
    private final MongoDocumentStore docStore;
    private final DocumentNodeStore documentNodeStore;
    private final RevisionVector rootRevision;
    private final BlobStore blobStore;
    private final File storeDir;
    private final Charset charset = UTF_8;
    private final Comparator<NodeStateHolder> comparator;
    private NotificationEmitter emitter;
    private final Compression algorithm;
    private long entryCount;
    private List<File> sortedFiles = new ArrayList<>();
    private Predicate<String> pathPredicate;

    private final ExecutorService dumpThread = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("mongo-dump").setDaemon(true).build()
    );
    private final ExecutorService sortThreadPool = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("mongo-sort").setDaemon(true).build()
    );

    PipelinedStrategy(MongoDocumentStore documentStore, DocumentNodeStore documentNodeStore, RevisionVector rootRevision, PathElementComparator pathComparator,
                      BlobStore blobStore,
                      File storeDir, Compression algorithm, Predicate<String> pathPredicate) {
        this.docStore = documentStore;
        this.documentNodeStore = documentNodeStore;
        this.rootRevision = rootRevision;
        this.blobStore = blobStore;
        this.storeDir = storeDir;
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.pathPredicate = pathPredicate;
        this.algorithm = algorithm;
    }

    private int getEnvVariableAsInt(String name, int defaultValue) {
        String value = System.getenv(name);
        int result;
        if (value == null) {
            result = defaultValue;
        } else {
            result = Integer.parseInt(value);
        }
        log.info("Config {}={}", name, result);
        return result;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        int mongoDocBlockQueueSize = getEnvVariableAsInt(MONGO_DOC_QUEUE_SIZE, 1000);
        int mongoBatchSize = getEnvVariableAsInt(MONGO_BLOCK_SIZE, 100);
        int nseBatchSize = getEnvVariableAsInt(PIPELINED_NSE_BATCH_SIZE, 1000);
        int transformThreads = getEnvVariableAsInt(PIPELINED_TRANSFORM_THREADS, 1);
        int sortBufferSize = getEnvVariableAsInt(PIPELINED_SORT_BUFFER_SIZE, 1_000_000);

        ExecutorService transformThreadPool = Executors.newFixedThreadPool(transformThreads,
                new ThreadFactoryBuilder().setNameFormat("mongo-transform-%d").setDaemon(true).build()
        );

        ArrayBlockingQueue<BasicDBObject[]> mongoDocQueue = new ArrayBlockingQueue<>(mongoDocBlockQueueSize);
        ArrayBlockingQueue<ArrayList<BasicNodeStateHolder>> entryListBlocksQueue = new ArrayBlockingQueue<>(1000);
        ArrayBlockingQueue<File> sortedFilesQueue = new ArrayBlockingQueue<>(1000);

        Stopwatch start = Stopwatch.createStarted();
        MongoDocumentTraverser.TraversingRange range = new MongoDocumentTraverser.TraversingRange(
                new LastModifiedRange(0, Long.MAX_VALUE),
                null
        );
        PipelineMongoDownloadTask downloadTask = new PipelineMongoDownloadTask(
                docStore,
                Collection.NODES,
                range,
                s -> true,
                mongoBatchSize,
                mongoDocQueue);
        Future<?> downloadTaskFuture = dumpThread.submit(downloadTask);

        ArrayList<Future<TransformResult>> transformFutures = new ArrayList<>(transformThreads);
        for (int i = 0; i < transformThreads; i++) {
            NodeStateEntryWriter entryWriter = new NodeStateEntryWriter(blobStore);
            PipelinedTransformTask transformTask = new PipelinedTransformTask(
                    docStore,
                    documentNodeStore,
                    Collection.NODES,
                    rootRevision,
                    pathPredicate,
                    entryWriter,
                    nseBatchSize,
                    mongoDocQueue,
                    entryListBlocksQueue
            );
            transformFutures.add(transformThreadPool.submit(transformTask));
        }

        PipelinedSortBatchTask sortTask = new PipelinedSortBatchTask(comparator,
                storeDir,
                new NodeStateEntryWriter(blobStore),
                algorithm,
                sortBufferSize,
                entryListBlocksQueue,
                sortedFilesQueue
        );
        Future<?> sortFuture = sortThreadPool.submit(sortTask);

        long entryCount = 0;
        try {
            log.info("Waiting for download task to finish. {}", downloadTaskFuture);
            downloadTaskFuture.get();
            // Signal the end of documents to the transform threads.
            for (int i = 0; i < transformThreads; i++) {
                mongoDocQueue.put(SENTINEL_MONGO_DOCUMENT);
            }
            for (Future<TransformResult> f : transformFutures) {
                log.info("Waiting for transform task to finish. {}", f);
                TransformResult transformResult = f.get();
                entryCount += transformResult.getEntryCount();
            }

            entryListBlocksQueue.put(SENTINEL_ENTRY_BATCH);
            log.info("Waiting for sort task to finish. {}", sortFuture);
            sortFuture.get();

            sortedFilesQueue.stream().iterator().forEachRemaining(sortedFiles::add);
            log.info("Dumped {} nodestates in json format in {}", entryCount, start);
            log.info("Created {} sorted files of size {} to merge",
                    sortedFiles.size(), humanReadableByteCount(sizeOf(sortedFiles)));

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.error("Error dumping from MongoDB", e);
            throw new RuntimeException(e);
        }

        return sortStoreFile(sortedFiles);
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    private File sortStoreFile(List<File> sortedFilesBatch) throws IOException {
        log.info("Proceeding to perform merge of {} sorted files", sortedFilesBatch.size());
        Stopwatch w = Stopwatch.createStarted();
        File sortedFile = new File(storeDir, getSortedStoreFileName(algorithm));
        try (BufferedWriter writer = createWriter(sortedFile, algorithm)) {
            Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
            Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(sortedFilesBatch,
                    writer,
                    comparator,
                    charset,
                    true, //distinct
                    algorithm,
                    func2,
                    func1
            );
        }
        log.info("Merging of sorted files completed in {}", w);
        return sortedFile;
    }
}
