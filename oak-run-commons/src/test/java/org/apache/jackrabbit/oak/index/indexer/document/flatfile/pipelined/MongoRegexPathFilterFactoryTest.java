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

import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class MongoRegexPathFilterFactoryTest {

    private static MongoRegexPathFilterFactory filter;
    PathFilter fullTree = new PathFilter(Set.of("/"), Set.of());
    PathFilter multipleExcludes = new PathFilter(Set.of("/"), Set.of("/excluded/sub1/sub2", "/excluded2", "/tmp"));
    PathFilter singleInclude = new PathFilter(Set.of("/included/sub"), Set.of());

    @BeforeClass
    public static void init() {
        filter = new MongoRegexPathFilterFactory(PipelinedMongoDownloadTask.DEFAULT_OAK_INDEXER_PIPELINED_MONGO_REGEX_PATH_FILTERING_MAX_PATHS);
    }

    @Test
    public void includeRootSingleExclude() {
        var f = filter.buildMongoFilter(List.of(fullTree), List.of("/excluded"));
        assertPaths(List.of("/"), List.of("/excluded"), f);
    }

    @Test
    public void includeRootMultipleExclude() {
        var f = filter.buildMongoFilter(List.of(fullTree), List.of("/excluded1", "/excluded2"));
        assertPaths(List.of("/"), List.of("/excluded1", "/excluded2"), f);
    }


    @Test
    public void includeRootMultipleExcludeSubtree() {
        var f = filter.buildMongoFilter(List.of(fullTree), List.of("/excluded", "/excluded/sub"));
        assertPaths(List.of("/"), List.of("/excluded"), f);
    }

    @Test
    public void customExcludeRootFails() {
        var f = filter.buildMongoFilter(List.of(fullTree), List.of("/"));
        assertPaths(List.of("/"), List.of(), f);
    }

    @Test
    public void customExcludeOutsideIncludeTree() {
        var f = filter.buildMongoFilter(List.of(singleInclude), List.of("/excluded"));
        assertPaths(List.of("/included/sub"), List.of(), f);
    }

    @Test
    public void customExcludeEqualsToIncludeTree() {
        var f = filter.buildMongoFilter(List.of(singleInclude), List.of("/included/sub"));
        assertPaths(List.of("/included/sub"), List.of(), f);
    }

    @Test
    public void customExcludeInsideIncludeTree() {
        var f = filter.buildMongoFilter(List.of(singleInclude), List.of("/included/sub/sub2"));
        assertPaths(List.of("/included/sub"), List.of(), f);
    }

    @Test
    public void customExcludeChildrenOfIndexExcludes() {
        var f = filter.buildMongoFilter(List.of(multipleExcludes), List.of("/excluded2", "/tmp/bin"));
        assertPaths(List.of("/"), multipleExcludes.getExcludedPaths(), f);
    }

    @Test
    public void customExcludeParentOfIndexExcludes() {
        var f = filter.buildMongoFilter(List.of(multipleExcludes), List.of("/excluded"));
        assertPaths(List.of("/"), List.of("/excluded", "/excluded2", "/tmp"), f);
    }

    private static void assertPaths(List<String> expectedIncluded, List<String> expectedExcluded, MongoRegexPathFilterFactory.MongoFilterPaths actual) {
        assertEquals(new HashSet<>(expectedIncluded), new HashSet<>(actual.included));
        assertEquals(new HashSet<>(expectedExcluded), new HashSet<>(actual.excluded));
    }
}
