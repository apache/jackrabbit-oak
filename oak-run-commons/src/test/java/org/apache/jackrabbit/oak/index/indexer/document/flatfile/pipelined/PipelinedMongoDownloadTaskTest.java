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
import com.mongodb.client.model.Filters;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.MongoRegexPathFilterFactory.MongoFilterPaths;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.MongoDownloaderRegexUtils.LONG_PATH_ID_PATTERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PipelinedMongoDownloadTaskTest {

    @Rule
    public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

    private MongoRegexPathFilterFactory regexFilterBuilder;

    @Before
    public void setUp() {
        this.regexFilterBuilder = new MongoRegexPathFilterFactory(PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS);
    }

    private List<PathFilter> createIncludedPathFilters(String... paths) {
        return Arrays.stream(paths).map(path -> new PathFilter(List.of(path), List.of())).collect(Collectors.toList());
    }

    @Test
    public void ancestorsFilters() {
        assertEquals(List.of(), MongoDownloaderRegexUtils.getAncestors(List.of()));
        assertEquals(List.of("/"), MongoDownloaderRegexUtils.getAncestors(List.of("/")));
        assertEquals(List.of("/", "/a"), MongoDownloaderRegexUtils.getAncestors(List.of("/a")));
        assertEquals(List.of("/", "/a", "/b", "/c", "/c/c1", "/c/c1/c2", "/c/c1/c2/c3", "/c/c1/c2/c4"),
                MongoDownloaderRegexUtils.getAncestors(List.of("/a", "/b", "/c/c1/c2/c3", "/c/c1/c2/c4"))
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
        assertNull(MongoDownloaderRegexUtils.compileCustomExcludedPatterns(null));
        assertNull(MongoDownloaderRegexUtils.compileCustomExcludedPatterns(""));

        Pattern p = Pattern.compile("^(?!.*(^[0-9]{1,3}:/a/b.*$)$)");
        var actualListOfPatterns = MongoDownloaderRegexUtils.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(actualListOfPatterns);

        assertEquals(p.toString(), actualListOfPatterns.toString());
    }

    @Test
    public void computeMongoQueryFilterNoPathFilterNoExcludeFilter() {
        // No path filter and no exclude filter
        assertNull(
                MongoDownloaderRegexUtils.computeMongoQueryFilter(
                        MongoFilterPaths.DOWNLOAD_ALL,
                        null
                )
        );
    }

    @Test
    public void computeMongoQueryFilterOptimizeWhenIncludedPathIsRoot() {
        // Path filter but no exclude filter
        var actual = MongoDownloaderRegexUtils.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/"), List.of("/excluded1", "/content/excluded2")),
                null
        );
        // The generated filter should not include any condition to include the descendants of /
        var expected = Filters.and(
                Filters.regex(NodeDocument.ID, MongoDownloaderRegexUtils.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/excluded1/"))),
                Filters.regex(NodeDocument.ID, MongoDownloaderRegexUtils.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/content/excluded2/"))
                ));
        assertBsonEquals(expected, actual);
    }


    @Test
    public void computeMongoQueryFilterWithPathFilterNoExcludeFilter() {
        // Path filter but no exclude filter
        var actual = MongoDownloaderRegexUtils.computeMongoQueryFilter(
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
        var actual = MongoDownloaderRegexUtils.computeMongoQueryFilter(
                MongoFilterPaths.DOWNLOAD_ALL,
                "^[0-9]{1,3}:/a/b.*$"
        );
        var customExcludedPattern = MongoDownloaderRegexUtils.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(customExcludedPattern);
        Bson expectedFilter = Filters.regex(NodeDocument.ID, customExcludedPattern);
        assertBsonEquals(expectedFilter, actual);
    }

    @Test
    public void computeMongoQueryFilterWithPathFilterWithExcludeFilter() {
        var actual = MongoDownloaderRegexUtils.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/parent"), List.of()),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludesPattern = MongoDownloaderRegexUtils.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
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
        var actual = MongoDownloaderRegexUtils.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/parent"), List.of()),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludePattern = MongoDownloaderRegexUtils.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
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
        var actual = MongoDownloaderRegexUtils.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/"), List.of("/excluded")),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludePattern = MongoDownloaderRegexUtils.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(excludePattern);
        var expected = Filters.and(
                Filters.regex(NodeDocument.ID, MongoDownloaderRegexUtils.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/excluded/"))),
                Filters.regex(NodeDocument.ID, excludePattern)
        );
        assertBsonEquals(expected, actual);
    }

    @Test
    public void computeMongoQueryFilterWithPathFilterWithExcludeFilterAndNaturalIndexTraversal() {
        var actual = MongoDownloaderRegexUtils.computeMongoQueryFilter(
                new MongoFilterPaths(List.of("/"), List.of("/excluded")),
                "^[0-9]{1,3}:/a/b.*$"
        );

        Pattern excludePattern = MongoDownloaderRegexUtils.compileCustomExcludedPatterns("^[0-9]{1,3}:/a/b.*$");
        assertNotNull(excludePattern);
        var expected = Filters.and(
                Filters.regex(NodeDocument.ID, MongoDownloaderRegexUtils.compileExcludedDirectoryRegex("^[0-9]{1,3}:" + Pattern.quote("/excluded/"))),
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

