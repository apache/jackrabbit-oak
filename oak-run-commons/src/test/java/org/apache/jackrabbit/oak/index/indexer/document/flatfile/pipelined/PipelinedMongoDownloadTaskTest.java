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

import com.mongodb.MongoClient;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.MongoRegexPathFilterFactory.MongoFilterPaths;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.index.ConsoleIndexingReporter;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedMongoDownloadTask.LONG_PATH_ID_PATTERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelinedMongoDownloadTaskTest {

    private MongoRegexPathFilterFactory regexFilterBuilder;

    private NodeDocument newBasicDBObject(String id, long modified, DocumentStore docStore) {
        NodeDocument obj = Collection.NODES.newDocument(docStore);
        obj.put(NodeDocument.ID, "3:/content/dam/asset" + id);
        obj.put(NodeDocument.MODIFIED_IN_SECS, modified);
        obj.put(NodeDocumentCodec.SIZE_FIELD, 100);
        return obj;
    }

    @Before
    public void setUp() {
        this.regexFilterBuilder = new MongoRegexPathFilterFactory(PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS);
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
                ConsoleIndexingReporter reporter = new ConsoleIndexingReporter();
                PipelinedMongoDownloadTask task = new PipelinedMongoDownloadTask(mongoDatabase, mongoDocumentStore,
                        batchMaxMemorySize, batchMaxElements, queue, null,
                        metricStatisticsProvider, reporter);

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
                        PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL,
                        PipelinedMetrics.OAK_INDEXER_PIPELINED_DOCUMENTS_DOWNLOADED_TOTAL_BYTES
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
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, regexFilterBuilder.buildMongoFilter(null));

        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, regexFilterBuilder.buildMongoFilter(List.of()));

        List<PathFilter> singlePathFilter = List.of(
                new PathFilter(List.of("/content/dam"), List.of())
        );
        assertEquals(new MongoFilterPaths(List.of("/content/dam"), List.of()), regexFilterBuilder.buildMongoFilter(singlePathFilter));

        List<PathFilter> multipleIncludeFilters = createIncludedPathFilters("/content/dam", "/content/dam");
        assertEquals(new MongoFilterPaths(List.of("/content/dam"), List.of()), regexFilterBuilder.buildMongoFilter(multipleIncludeFilters));

        List<PathFilter> includesRoot = createIncludedPathFilters("/", "/a/a1/a2");
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, regexFilterBuilder.buildMongoFilter(includesRoot));

        List<PathFilter> multipleIncludeFiltersDifferent = createIncludedPathFilters("/a/a1", "/a/a1/a2");
        assertEquals(new MongoFilterPaths(List.of("/a/a1"), List.of()), regexFilterBuilder.buildMongoFilter(multipleIncludeFiltersDifferent));

        List<PathFilter> multipleIncludeFiltersDifferent2 = createIncludedPathFilters("/a/a1/a2", "/a/a1", "/b", "/c", "/cc");
        assertEquals(new MongoFilterPaths(List.of("/a/a1", "/b", "/c", "/cc"), List.of()), regexFilterBuilder.buildMongoFilter(multipleIncludeFiltersDifferent2));
    }

    @Test
    public void regexFiltersIncludedAndExcludedPaths() {
        // Excludes is not empty

        // Exclude paths is inside the included path tree
        List<PathFilter> withExcludeFilter1 = List.of(
                new PathFilter(List.of("/a"), List.of("/a/b"))
        );
        assertEquals(new MongoFilterPaths(List.of("/a"), List.of("/a/b")), regexFilterBuilder.buildMongoFilter(withExcludeFilter1));

        // includedPath contains the root
        List<PathFilter> withExcludeFilter2 = List.of(
                new PathFilter(List.of("/"), List.of("/a"))
        );
        assertEquals(new MongoFilterPaths(List.of("/"), List.of("/a")), regexFilterBuilder.buildMongoFilter(withExcludeFilter2));

        // One of the filters excludes a directory that is included by another filter. The path should still be included.
        List<PathFilter> withExcludeFilter3_a = List.of(
                new PathFilter(List.of("/"), List.of("/a")),
                new PathFilter(List.of("/"), List.of())
        );
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, regexFilterBuilder.buildMongoFilter(withExcludeFilter3_a));

        List<PathFilter> withExcludeFilter3_b = List.of(
                new PathFilter(List.of("/a"), List.of("/a/a_excluded")),
                new PathFilter(List.of("/a"), List.of())
        );
        assertEquals(new MongoFilterPaths(List.of("/a"), List.of()), regexFilterBuilder.buildMongoFilter(withExcludeFilter3_b));

        List<PathFilter> withExcludeFilter4 = List.of(
                new PathFilter(List.of("/"), List.of("/exc_a")),
                new PathFilter(List.of("/"), List.of("/exc_b"))
        );
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, regexFilterBuilder.buildMongoFilter(withExcludeFilter4));

        List<PathFilter> withExcludeFilter5 = List.of(
                new PathFilter(List.of("/a"), List.of("/a/a_excluded")),
                new PathFilter(List.of("/b"), List.of("/b/b_excluded"))
        );
        assertEquals(new MongoFilterPaths(List.of("/a", "/b"), List.of("/a/a_excluded", "/b/b_excluded")), regexFilterBuilder.buildMongoFilter(withExcludeFilter5));

        List<PathFilter> withExcludeFilter6 = List.of(
                new PathFilter(List.of("/"), List.of("/a", "/b", "/c")),
                new PathFilter(List.of("/"), List.of("/b", "/c", "/d"))
        );
        assertEquals(new MongoFilterPaths(List.of("/"), List.of("/b", "/c")), regexFilterBuilder.buildMongoFilter(withExcludeFilter6));

        List<PathFilter> withExcludeFilter7 = List.of(
                new PathFilter(List.of("/a"), List.of("/a/b")),
                new PathFilter(List.of("/a"), List.of("/a/b/c"))
        );
        assertEquals(new MongoFilterPaths(List.of("/"), List.of("/b", "/c")), regexFilterBuilder.buildMongoFilter(withExcludeFilter6));
        assertEquals(new MongoFilterPaths(List.of("/a"), List.of("/a/b/c")), regexFilterBuilder.buildMongoFilter(withExcludeFilter7));

        List<PathFilter> withExcludeFilter8 = List.of(
                new PathFilter(
                        List.of("/p1", "/p2", "/p3", "/p4", "/p5", "/p6", "/p7", "/p8", "/p9"),
                        List.of("/p10", "/p5/p5_s1", "/p5/p5_s2/p5_s3", "/p11")
                )
        );
        assertEquals(new MongoFilterPaths(
                        List.of("/p1", "/p2", "/p3", "/p4", "/p5", "/p6", "/p7", "/p8", "/p9"),
                        List.of("/p5/p5_s1", "/p5/p5_s2/p5_s3")),
                regexFilterBuilder.buildMongoFilter(withExcludeFilter8));
    }


    @Test
    public void regexFiltersLargeNumberOfIncludedPaths1() {
        // Test a single path filter with many included paths.
        var maximumPaths = IntStream.range(0, PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS)
                .mapToObj(i -> "/p" + i)
                .collect(Collectors.toList());
        var pathFilter1 = List.of(new PathFilter(maximumPaths, List.of()));
        assertEquals(new MongoFilterPaths(maximumPaths.stream().sorted().collect(Collectors.toList()), List.of()),
                regexFilterBuilder.buildMongoFilter(pathFilter1));


        var tooManyPaths = IntStream.range(0, PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS + 1)
                .mapToObj(i -> "/p" + i)
                .collect(Collectors.toList());
        var pathFilter2 = List.of(new PathFilter(tooManyPaths, List.of()));
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, regexFilterBuilder.buildMongoFilter(pathFilter2));
    }

    @Test
    public void regexFiltersLargeNumberOfIncludedPaths2() {
        // Test many path filters each with a single included paths.
        var pathFilters = IntStream.range(0, PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS + 1)
                .mapToObj(i -> new PathFilter(List.of("/p" + i), List.of()))
                .collect(Collectors.toList());
        assertEquals(MongoFilterPaths.DOWNLOAD_ALL, regexFilterBuilder.buildMongoFilter(pathFilters));
    }

    @Test
    public void regexFiltersLargeNumberOfExcludedPaths() {
        // When there are too many excluded paths, do not filter on excluded paths but still filter on included paths.
        var excludedPaths = IntStream.range(0, PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS + 1)
                .mapToObj(i -> "/parent/p" + i)
                .collect(Collectors.toList());
        var pathFilter = List.of(new PathFilter(List.of("/parent"), excludedPaths));
        assertEquals(new MongoFilterPaths(List.of("/parent"), List.of()), regexFilterBuilder.buildMongoFilter(pathFilter));
    }

    @Test
    public void createCustomExcludeEntriesFilter() {
        assertNull(PipelinedMongoDownloadTask.compileCustomExcludedPatterns(null));
        assertNull(PipelinedMongoDownloadTask.compileCustomExcludedPatterns(""));

        Pattern p = Pattern.compile("^(?!.*(^[0-9]{1,3}:/a/b.*$)$)");
        var actualListOfPatterns = PipelinedMongoDownloadTask.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(actualListOfPatterns);

        assertEquals(p.toString(), actualListOfPatterns.toString());
    }

    @Test
    public void computeMongoQueryFilterNoPathFilterNoExcludeFilter() {
        // No path filter and no exclude filter
        assertNull(
                PipelinedMongoDownloadTask.computeMongoQueryFilter(
                        MongoFilterPaths.DOWNLOAD_ALL,
                        null
                )
        );
    }

    @Test
    public void computeMongoQueryFilterOptimizeWhenIncludedPathIsRoot() {
        // Path filter but no exclude filter
        var actual = PipelinedMongoDownloadTask.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/"), List.of("/excluded1", "/content/excluded2")),
                null
        );
        // The generated filter should not include any condition to include the descendants of /
        var expected = Filters.and(
                Filters.regex(NodeDocument.ID, PipelinedMongoDownloadTask.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/excluded1/"))),
                Filters.regex(NodeDocument.ID, PipelinedMongoDownloadTask.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/content/excluded2/"))
                ));
        assertBsonEquals(expected, actual);
    }


    @Test
    public void computeMongoQueryFilterWithPathFilterNoExcludeFilter() {
        // Path filter but no exclude filter
        var actual = PipelinedMongoDownloadTask.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/parent"), List.of()),
                null
        );
        var expected = Filters.in(NodeDocument.ID,
                Pattern.compile("^[0-9]{1,3}:" + Pattern.quote("/parent/")),
                LONG_PATH_ID_PATTERN
        );
        assertBsonEquals(expected, actual);
    }

    @Test
    public void computeMongoQueryFilterNoPathFilterWithExcludeFilter() {
        var actual = PipelinedMongoDownloadTask.computeMongoQueryFilter(
                MongoFilterPaths.DOWNLOAD_ALL,
                "^[0-9]{1,3}:/a/b.*$"
        );
        var customExcludedPattern = PipelinedMongoDownloadTask.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(customExcludedPattern);
        Bson expectedFilter = Filters.regex(NodeDocument.ID, customExcludedPattern);
        assertBsonEquals(expectedFilter, actual);
    }

    @Test
    public void computeMongoQueryFilterWithPathFilterWithExcludeFilter() {
        var actual = PipelinedMongoDownloadTask.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/parent"), List.of()),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludesPattern = PipelinedMongoDownloadTask.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(excludesPattern);
        var expected =
                Filters.and(
                        Filters.in(NodeDocument.ID, Pattern.compile("^[0-9]{1,3}:" + Pattern.quote("/parent/")), LONG_PATH_ID_PATTERN),
                        Filters.regex(NodeDocument.ID, excludesPattern)
                );
        assertBsonEquals(expected, actual);
    }

    @Test
    public void computeMongoQueryFilterWithPathFilterWithExcludeFilterAndNaturalOrderTraversal() {
        var actual = PipelinedMongoDownloadTask.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/parent"), List.of()),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludePattern = PipelinedMongoDownloadTask.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(excludePattern);
        var expected =
                Filters.and(
                        Filters.in(NodeDocument.ID,
                                Pattern.compile("^[0-9]{1,3}:" + Pattern.quote("/parent/")),
                                LONG_PATH_ID_PATTERN
                        ),
                        Filters.regex(NodeDocument.ID, excludePattern)
                );
        assertBsonEquals(expected, actual);
    }

    @Test
    public void computeMongoQueryFilterWithPathFilterWithExcludeFilterAndNaturalColumnTraversal() {
        var actual = PipelinedMongoDownloadTask.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/"), List.of("/excluded")),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludePattern = PipelinedMongoDownloadTask.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(excludePattern);
        var expected = Filters.and(
                Filters.regex(NodeDocument.ID, PipelinedMongoDownloadTask.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/excluded/"))),
                Filters.regex(NodeDocument.ID, excludePattern)
        );
        assertBsonEquals(expected, actual);
    }

    @Test
    public void computeMongoQueryFilterWithPathFilterWithExcludeFilterAndNaturalIndexTraversal() {
        var actual = PipelinedMongoDownloadTask.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/"), List.of("/excluded")),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludePattern = PipelinedMongoDownloadTask.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(excludePattern);
        var expected = Filters.and(
                Filters.regex(NodeDocument.ID, PipelinedMongoDownloadTask.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/excluded/"))),
                Filters.regex(NodeDocument.ID, excludePattern)
        );
        assertBsonEquals(expected, actual);
    }

    private void assertBsonEquals(Bson actual, Bson expected) {
        if (actual == null && expected == null) {
            return;
        } else if (actual == null || expected == null) {
            throw new AssertionError("One of the bson is null. Actual: " + actual + ", expected: " + expected);
        }
        assertEquals(
                actual.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry()),
                expected.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry())
        );
    }

    @Test
    public void mergeExcludedPaths() {
        // Indexes do not define any exclude, the custom index adds one exclude
        testMergeExcludedPaths(List.of(), List.of("/b"), List.of("/b"));
        // Indexes define one exclude, the custom index adds one exclude
        testMergeExcludedPaths(List.of("/a"), List.of("/b"), List.of("/a", "/b"));
        // Adds an exclude already in the list
        testMergeExcludedPaths(List.of("/a"), List.of("/a"), List.of("/a"));
        // Adds an exclude that is a child of an existing exclude
        testMergeExcludedPaths(List.of("/a"), List.of("/a/s"), List.of("/a"));
        // Adds an exclude that is a parent of an existing exclude
        testMergeExcludedPaths(List.of("/a/s"), List.of("/a"), List.of("/a"));
        testMergeExcludedPaths(List.of("/a/s"), List.of(), List.of("/a/s"));
        testMergeExcludedPaths(List.of("/a", "/b/b1", "/c/c1/c2"), List.of("/d/d1/d2", "/b", "/c/c1/c2/c3"),
                List.of("/a", "/b", "/c/c1/c2", "/d/d1/d2"));
        testMergeExcludedPaths(List.of("/a/s"), List.of("/"), List.of("/"));

    }

    private void testMergeExcludedPaths(List<String> indexExcludedPaths, List<String> customExcludedPaths, List<String> expected) {
        var mergedExcludedPaths = MongoRegexPathFilterFactory.mergeIndexAndCustomExcludePaths(indexExcludedPaths, customExcludedPaths);
        assertEquals(expected, mergedExcludedPaths);
    }
}

