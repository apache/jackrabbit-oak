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
import static org.junit.Assert.assertNull;
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
            assertEquals(metricNames, Set.of(PipelinedMetrics.OAK_INDEXER_PIPELINED_MONGO_DOWNLOAD_ENQUEUE_DELAY_PERCENTAGE));
        }

        verify(dbCollection).find(BsonDocument.parse("{\"_modified\": {\"$gte\": 0}}"));
        verify(dbCollection).find(BsonDocument.parse("{\"_modified\": {\"$gte\": 123000, \"$lt\": 123001}, \"_id\": {\"$gt\": \"3:/content/dam/asset1\"}}"));
        verify(dbCollection).find(BsonDocument.parse("{\"_modified\": {\"$gte\": 123001}}"));
    }

    @Test
    public void regexFilters() {
        assertNull(PipelinedMongoDownloadTask.getSingleIncludedPath(null));

        assertNull(PipelinedMongoDownloadTask.getSingleIncludedPath(List.of()));

        List<PathFilter> singlePathFilter = List.of(
                new PathFilter(List.of("/content/dam"), List.of())
        );
        assertEquals("/content/dam", PipelinedMongoDownloadTask.getSingleIncludedPath(singlePathFilter));

        List<PathFilter> multipleIncludeFilters = List.of(
                new PathFilter(List.of("/content/dam"), List.of()),
                new PathFilter(List.of("/content/dam"), List.of())
        );
        assertEquals("/content/dam", PipelinedMongoDownloadTask.getSingleIncludedPath(multipleIncludeFilters));

        List<PathFilter> multipleIncludeFiltersDifferent = List.of(
                new PathFilter(List.of("/content/dam"), List.of()),
                new PathFilter(List.of("/content/dam/collections"), List.of())
        );
        assertNull(PipelinedMongoDownloadTask.getSingleIncludedPath(multipleIncludeFiltersDifferent));

        List<PathFilter> withExcludeFilter = List.of(
                new PathFilter(List.of("/"), List.of("/var"))
        );
        assertNull(PipelinedMongoDownloadTask.getSingleIncludedPath(withExcludeFilter));
    }
}