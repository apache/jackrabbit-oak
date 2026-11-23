/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreHelper;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.SENTINEL_MONGO_DOCUMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class PipelinedMongoDownloadTaskCancellationTest {
    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    @Rule
    public final MockitoRule mocks = MockitoJUnit.rule();

    @Mock
    private MongoClient client;
    @Mock
    private MongoDatabase database;
    @Mock
    private MongoCollection<RawBsonDocument> collection;
    @Mock(answer = Answers.RETURNS_SELF)
    private FindIterable<RawBsonDocument> iterable;
    @Mock
    private MongoCursor<RawBsonDocument> ascending;
    @Mock
    private MongoCursor<RawBsonDocument> descending;
    @Mock
    private MongoCursor<RawBsonDocument> updated;
    @Mock
    private MongoCursor<RawBsonDocument> empty;

    @Test(timeout = 20000)
    public void flushCancelledDescendingDownloadBeforeSentinel() throws Exception {
        cancelledDownload(false, CancellationResult.FLUSH);
    }

    @Test(timeout = 20000)
    public void flushCancelledAscendingDownloadBeforeSentinel() throws Exception {
        cancelledDownload(true, CancellationResult.FLUSH);
    }

    @Test(timeout = 20000)
    public void cancelledDownloadFailureAbortsPipeline() throws Exception {
        cancelledDownload(false, CancellationResult.FAILURE);
    }

    @Test(timeout = 20000)
    public void cancelledDownloadTimeoutAbortsPipeline() throws Exception {
        cancelledDownload(false, CancellationResult.TIMEOUT);
    }

    private enum CancellationResult {
        FLUSH, FAILURE, TIMEOUT
    }

    private void cancelledDownload(boolean cancelAscending, CancellationResult cancellationResult) throws Exception {
        System.setProperty(PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP, "true");
        System.setProperty(PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_PARALLEL_DUMP_SECONDARIES_ONLY, "false");
        System.setProperty(PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING, "true");
        System.setProperty(PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_RETRY_ON_CONNECTION_ERRORS, "true");
        System.setProperty(PipelinedMongoDownloadTask.OAK_INDEXER_PIPELINED_MONGO_CONNECTION_RETRY_SECONDS,
                cancellationResult == CancellationResult.TIMEOUT ? "1" : "5");

        when(client.getDatabase("oak")).thenReturn(database);
        when(database.getCollection("nodes", RawBsonDocument.class)).thenReturn(collection);
        when(collection.find(any(Bson.class))).thenReturn(iterable);
        when(iterable.iterator()).thenAnswer(invocation -> {
            switch (Thread.currentThread().getName()) {
                case "mongo-dump-ascending":
                    return ascending;
                case "mongo-dump-descending":
                    return descending;
                case "mongo-dump-updated-docs":
                    return updated;
                default:
                    return empty;
            }
        });

        CountDownLatch buffered = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        CompletableFuture<Void> releaseCursor = new CompletableFuture<>();
        CountDownLatch sentinel = new CountDownLatch(1);
        LinkedBlockingQueue<RawBsonDocument[]> queue = new LinkedBlockingQueue<>() {
            @Override
            public void put(RawBsonDocument[] documents) throws InterruptedException {
                if (documents == SENTINEL_MONGO_DOCUMENT) {
                    sentinel.countDown();
                }
                super.put(documents);
            }
        };
        MongoCursor<RawBsonDocument> blockedCursor = cancelAscending ? ascending : descending;
        MongoCursor<RawBsonDocument> crossingCursor = cancelAscending ? descending : ascending;
        long modified = cancelAscending ? 10 : 20;
        RawBsonDocument bufferedDocument = document("buffered", modified);
        RawBsonDocument crossingDocument = document("crossing", modified + (cancelAscending ? -1 : 1));
        RawBsonDocument newestDocument = document("newest", 30);
        IllegalArgumentException cursorFailure = new IllegalArgumentException("Cancelled cursor failed");
        Iterator<RawBsonDocument> pendingDocuments = (cancelAscending ?
                List.of(bufferedDocument) : List.of(newestDocument, bufferedDocument)).iterator();
        when(blockedCursor.next()).thenAnswer(invocation -> pendingDocuments.next());
        when(blockedCursor.hasNext()).thenAnswer(invocation -> {
            if (pendingDocuments.hasNext()) {
                return true;
            }
            buffered.countDown();
            // An in-flight getMore may outlive both cursor.close() and a thread interruption.
            releaseCursor.join();
            if (cancellationResult == CancellationResult.FAILURE) {
                throw cursorFailure;
            }
            return false;
        });
        doAnswer(invocation -> {
            cancelled.countDown();
            return null;
        }).when(blockedCursor).close();
        Iterator<RawBsonDocument> crossingDocuments = (cancelAscending ?
                List.of(newestDocument, crossingDocument) : List.of(crossingDocument)).iterator();
        when(crossingCursor.hasNext()).thenAnswer(invocation -> {
            assertTrue("Other stream did not buffer its document", buffered.await(10, TimeUnit.SECONDS));
            return crossingDocuments.hasNext();
        });
        when(crossingCursor.next()).thenAnswer(invocation -> crossingDocuments.next());
        when(updated.hasNext()).thenReturn(true, true, false);
        when(updated.next()).thenReturn(newestDocument);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> release = executor.submit(() -> {
            try {
                assertTrue("Buffered stream was not cancelled", cancelled.await(10, TimeUnit.SECONDS));
                if (cancellationResult == CancellationResult.FLUSH) {
                    assertFalse("End-of-input overtook the cancelled stream's buffered document",
                            sentinel.await(2, TimeUnit.SECONDS));
                }
            } finally {
                if (cancellationResult != CancellationResult.TIMEOUT) {
                    releaseCursor.complete(null);
                }
            }
            return null;
        });
        String originalThreadName = Thread.currentThread().getName();
        List<Thread> downloadThreads = new CopyOnWriteArrayList<>();
        MongoDocumentStore store = mock(MongoDocumentStore.class);
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class);
             MockedStatic<MongoDocumentStoreHelper> helper = mockStatic(MongoDocumentStoreHelper.class)) {
            clients.when(() -> MongoClients.create(any(MongoClientSettings.class))).thenReturn(client);
            helper.when(() -> MongoDocumentStoreHelper.getMongoDatabaseName(store)).thenReturn("oak");
            PipelinedMongoDownloadTask task = new PipelinedMongoDownloadTask(
                    new ConnectionString("mongodb://localhost"), store, 4 * 1024 * 1024, 10000, queue,
                    List.of(new PathFilter(List.of("/a/b"), List.of())), StatisticsProvider.NOOP,
                    mock(IndexingReporter.class), runnable -> {
                        Thread thread = new Thread(runnable);
                        downloadThreads.add(thread);
                        return thread;
                    });

            if (cancellationResult == CancellationResult.FLUSH) {
                PipelinedMongoDownloadTask.Result result = task.call();
                List<RawBsonDocument> downloaded = new ArrayList<>();
                RawBsonDocument[] batch;
                while ((batch = queue.poll()) != SENTINEL_MONGO_DOCUMENT) {
                    assertNotNull("Download did not signal end-of-input", batch);
                    downloaded.addAll(List.of(batch));
                }
                assertEquals(cancelAscending ?
                        List.of(newestDocument, crossingDocument, newestDocument, bufferedDocument) :
                        List.of(crossingDocument, newestDocument, newestDocument, bufferedDocument), downloaded);
                assertEquals(downloaded.size(), result.getDocumentsDownloaded());
                assertTrue("Documents were delivered after end-of-input", queue.isEmpty());
            } else {
                if (cancellationResult == CancellationResult.FAILURE) {
                    RuntimeException failure = assertThrows(RuntimeException.class, task::call);
                    assertSame(cursorFailure, failure.getCause().getCause());
                } else {
                    TimeoutException failure = assertThrows(TimeoutException.class, task::call);
                    assertTrue(failure.getMessage().contains("flushing pending documents within 1 seconds"));
                }
                assertEquals("Failed download must not signal end-of-input", 1, sentinel.getCount());
            }
            release.get(10, TimeUnit.SECONDS);
        } finally {
            Thread.currentThread().setName(originalThreadName);
            releaseCursor.complete(null);
            executor.shutdownNow();
            assertTrue("Cursor controller did not terminate", executor.awaitTermination(10, TimeUnit.SECONDS));
            for (Thread thread : downloadThreads) {
                thread.join(TimeUnit.SECONDS.toMillis(10));
                assertFalse("Download worker did not terminate", thread.isAlive());
            }
        }
    }

    private static RawBsonDocument document(String name, long modified) {
        return RawBsonDocument.parse("{_id: '3:/a/b/" + name + "', _modified: {$numberLong: '" + modified + "'}}");
    }
}
