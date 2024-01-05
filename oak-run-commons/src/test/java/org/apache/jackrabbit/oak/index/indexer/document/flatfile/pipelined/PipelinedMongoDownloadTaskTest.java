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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.MongoFilterPaths;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelinedMongoDownloadTaskTest {

    private NodeDocument newBasicDBObject(String id, long modified, DocumentStore docStore) {
        NodeDocument obj = Collection.NODES.newDocument(docStore);
        obj.put(NodeDocument.ID, "3:/content/dam/asset" + id);
        obj.put(NodeDocument.MODIFIED_IN_SECS, modified);
        obj.put(NodeDocumentCodec.SIZE_FIELD, 100);
        return obj;
    }

    @Test
    public void connectionToMongoFailure() throws Exception {
        @SuppressWarnings("unchecked")
        MongoCollection<NodeDocument> dbCollection = mock(MongoCollection.class);

        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        when(mongoDatabase.withCodecRegistry(any())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(eq(Collection.NODES.toString()), eq(NodeDocument.class))).thenReturn(dbCollection);

        DocumentStore docStore = mock(DocumentStore.class);
        List<NodeDocument> documents = List.of(
                newBasicDBObject("1", 123_000, docStore),
                newBasicDBObject("2", 123_000, docStore),
                newBasicDBObject("3", 123_001, docStore),
                newBasicDBObject("4", 123_002, docStore));

        @SuppressWarnings("unchecked")
        MongoCursor<NodeDocument> cursor = mock(MongoCursor.class);
        when(cursor.hasNext())
                .thenReturn(true)
                .thenThrow(new MongoSocketException("test", new ServerAddress()))
                .thenReturn(true, false) // response to the query that will finish downloading the documents with _modified = 123_000
                .thenReturn(true, true, false); // response to the query that downloads everything again starting from _modified >= 123_001
        when(cursor.next()).thenReturn(
                documents.get(0),
                documents.subList(1, documents.size()).toArray(new NodeDocument[0])
        );

        @SuppressWarnings("unchecked")
        FindIterable<NodeDocument> findIterable = mock(FindIterable.class);
        when(findIterable.sort(any())).thenReturn(findIterable);
        when(findIterable.iterator()).thenReturn(cursor);

        when(dbCollection.withReadPreference(any())).thenReturn(dbCollection);
        when(dbCollection.find()).thenReturn(findIterable);
        when(dbCollection.find(any(Bson.class))).thenReturn(findIterable);

        int batchMaxMemorySize = 512;
        int batchMaxElements = 10;
        BlockingQueue<NodeDocument[]> queue = new ArrayBlockingQueue<>(100);
        MongoDocumentStore mongoDocumentStore = mock(MongoDocumentStore.class);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            try (MetricStatisticsProvider metricStatisticsProvider = new MetricStatisticsProvider(null, executor)) {
                PipelinedMongoDownloadTask task = new PipelinedMongoDownloadTask(mongoDatabase, mongoDocumentStore,
                        batchMaxMemorySize, batchMaxElements, queue, null,
                        metricStatisticsProvider);

                // Execute
                PipelinedMongoDownloadTask.Result result = task.call();

                // Verify results
                assertEquals(documents.size(), result.getDocumentsDownloaded());
                ArrayList<NodeDocument[]> c = new ArrayList<>();
                queue.drainTo(c);
                List<NodeDocument> actualDocuments = c.stream().flatMap(Arrays::stream).collect(Collectors.toList());
                assertEquals(documents, actualDocuments);

                Set<String> metricNames = metricStatisticsProvider.getRegistry().getCounters().keySet();
                assertEquals(metricNames, Set.of(
                        PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE,
                        PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_DURATION_SECONDS,
                        PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL
                ));
            }
        } finally {
            executor.shutdown();
        }

        verify(dbCollection).find(BsonDocument.parse("{\"_modified\": {\"$gte\": 0}}"));
        verify(dbCollection).find(BsonDocument.parse("{\"_modified\": {\"$gte\": 123000, \"$lt\": 123001}, \"_id\": {\"$gt\": \"3:/content/dam/asset1\"}}"));
        verify(dbCollection).find(BsonDocument.parse("{\"_modified\": {\"$gte\": 123001}}"));
    }

    private List<PathFilter> createIncludedPathFilters(String... paths) {
        return Arrays.stream(paths).map(path -> new PathFilter(List.of(path), List.of())).collect(Collectors.toList());
    }

    @Test
    public void ancestorsFilters() {
        assertEquals(List.of(), PipelinedMongoDownloadTask.getAncestors(List.of()));
        assertEquals(List.of("/"), PipelinedMongoDownloadTask.getAncestors(List.of("/")));
        assertEquals(List.of("/", "/a"), PipelinedMongoDownloadTask.getAncestors(List.of("/a")));
        assertEquals(List.of("/", "/a", "/b", "/c", "/c/c1", "/c/c1/c2", "/c/c1/c2/c3", "/c/c1/c2/c4"),
                PipelinedMongoDownloadTask.getAncestors(List.of("/a", "/b", "/c/c1/c2/c3", "/c/c1/c2/c4"))
        );
    }

    @Test
    public void regexFiltersIncludedPathsOnly() {
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, PipelinedMongoDownloadTask.buildMongoFilter(null));

        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, PipelinedMongoDownloadTask.buildMongoFilter(List.of()));

        List<PathFilter> singlePathFilter = List.of(
                new PathFilter(List.of("/content/dam"), List.of())
        );
        assertEquals(new MongoFilterPaths(List.of("/content/dam"), List.of()), PipelinedMongoDownloadTask.buildMongoFilter(singlePathFilter));

        List<PathFilter> multipleIncludeFilters = createIncludedPathFilters("/content/dam", "/content/dam");
        assertEquals(new MongoFilterPaths(List.of("/content/dam"), List.of()), PipelinedMongoDownloadTask.buildMongoFilter(multipleIncludeFilters));

        List<PathFilter> includesRoot = createIncludedPathFilters("/", "/a/a1/a2");
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, PipelinedMongoDownloadTask.buildMongoFilter(includesRoot));

        List<PathFilter> multipleIncludeFiltersDifferent = createIncludedPathFilters("/a/a1", "/a/a1/a2");
        assertEquals(new MongoFilterPaths(List.of("/a/a1"), List.of()), PipelinedMongoDownloadTask.buildMongoFilter(multipleIncludeFiltersDifferent));

        List<PathFilter> multipleIncludeFiltersDifferent2 = createIncludedPathFilters("/a/a1/a2", "/a/a1", "/b", "/c", "/cc");
        assertEquals(new MongoFilterPaths(List.of("/a/a1", "/b", "/c", "/cc"), List.of()), PipelinedMongoDownloadTask.buildMongoFilter(multipleIncludeFiltersDifferent2));
    }

    @Test
    public void regexFiltersIncludedAndExcludedPaths() {
        // Excludes is not empty

        // Exclude paths is inside the included path tree
        List<PathFilter> withExcludeFilter1 = List.of(
                new PathFilter(List.of("/a"), List.of("/a/b"))
        );
        assertEquals(new MongoFilterPaths(List.of("/a"), List.of("/a/b")), PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter1));

        // includedPath contains the root
        List<PathFilter> withExcludeFilter2 = List.of(
                new PathFilter(List.of("/"), List.of("/a"))
        );
        assertEquals(new MongoFilterPaths(List.of("/"), List.of("/a")), PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter2));

        // One of the filters excludes a directory that is included by another filter. The path should still be included.
        List<PathFilter> withExcludeFilter3_a = List.of(
                new PathFilter(List.of("/"), List.of("/a")),
                new PathFilter(List.of("/"), List.of())
        );
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter3_a));

        List<PathFilter> withExcludeFilter3_b = List.of(
                new PathFilter(List.of("/a"), List.of("/a/a_excluded")),
                new PathFilter(List.of("/a"), List.of())
        );
        assertEquals(new MongoFilterPaths(List.of("/a"), List.of()), PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter3_b));

        List<PathFilter> withExcludeFilter4 = List.of(
                new PathFilter(List.of("/"), List.of("/exc_a")),
                new PathFilter(List.of("/"), List.of("/exc_b"))
        );
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter4));

        List<PathFilter> withExcludeFilter5 = List.of(
                new PathFilter(List.of("/a"), List.of("/a/a_excluded")),
                new PathFilter(List.of("/b"), List.of("/b/b_excluded"))
        );
        assertEquals(new MongoFilterPaths(List.of("/a", "/b"), List.of("/a/a_excluded", "/b/b_excluded")), PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter5));

        List<PathFilter> withExcludeFilter6 = List.of(
                new PathFilter(List.of("/"), List.of("/a", "/b", "/c")),
                new PathFilter(List.of("/"), List.of("/b", "/c", "/d"))
        );
        assertEquals(new MongoFilterPaths(List.of("/"), List.of("/b", "/c")), PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter6));

        List<PathFilter> withExcludeFilter7 = List.of(
                new PathFilter(List.of("/a"), List.of("/a/b")),
                new PathFilter(List.of("/a"), List.of("/a/b/c"))
        );
        assertEquals(new MongoFilterPaths(List.of("/"), List.of("/b", "/c")), PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter6));
        assertEquals(new MongoFilterPaths(List.of("/a"), List.of("/a/b/c")), PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter7));

        List<PathFilter> withExcludeFilter8 = List.of(
                new PathFilter(
                        List.of("/p1", "/p2", "/p3", "/p4", "/p5", "/p6", "/p7", "/p8", "/p9"),
                        List.of("/p10", "/p5/p5_s1", "/p5/p5_s2/p5_s3", "/p11")
                )
        );
        assertEquals(new MongoFilterPaths(
                        List.of("/p1", "/p2", "/p3", "/p4", "/p5", "/p6", "/p7", "/p8", "/p9"),
                        List.of("/p5/p5_s1", "/p5/p5_s2/p5_s3")),
                PipelinedMongoDownloadTask.buildMongoFilter(withExcludeFilter8));
    }
}