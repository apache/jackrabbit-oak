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

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.AheadOfTimeBlobDownloadingFlatFileStore.filterEnabledIndexes;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.AheadOfTimeBlobDownloadingFlatFileStore.isEnabledForAnyOfIndexes;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.AheadOfTimeBlobDownloadingFlatFileStore.splitAndTrim;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class AheadOfTimeBlobDownloadingFlatFileStoreTest {

    @Test
    public void testIsEnabledForAnyOfIndexes() {
        assertFalse(isEnabledForAnyOfIndexes(List.of(), List.of("/oak:index/fooA-34")));
        assertTrue(isEnabledForAnyOfIndexes(List.of("/oak:index/foo"), List.of("/oak:index/fooA-34")));
        assertTrue(isEnabledForAnyOfIndexes(List.of("/oak:index/foo"), List.of("/oak:index/anotherIndex", "/oak:index/fooA-34")));
        assertFalse(isEnabledForAnyOfIndexes(List.of("/oak:index/foo"), List.of("/oak:index/anotherIndex")));
        assertTrue(isEnabledForAnyOfIndexes(List.of("/oak:index/fooA-", "/oak:index/fooB-"), List.of("/oak:index/fooA-34")));
        assertTrue(isEnabledForAnyOfIndexes(List.of("/oak:index/fooA-", "/oak:index/fooB-"), List.of("/oak:index/anotherIndex", "/oak:index/fooA-34")));
        assertFalse(isEnabledForAnyOfIndexes(List.of("/oak:index/fooA-"), List.of()));
    }

    @Test
    public void testFilterEnabledIndexes() {
        doFilterEnabledIndexesTest(List.of(), // expected enabled indexes
                List.of(), // prefix of indexes for which it is enabled
                List.of("/oak:index/fooA-34" // all indexes
                ));
        doFilterEnabledIndexesTest(List.of("/oak:index/fooA-34"),
                List.of("/oak:index"), List.of("/oak:index/fooA-34"));
        doFilterEnabledIndexesTest(List.of("/oak:index/fooA-34", "/oak:index/fooB-34"),
                List.of("/oak:index"), List.of("/oak:index/fooA-34", "/oak:index/fooB-34"));
        doFilterEnabledIndexesTest(List.of("/oak:index/fooA-34", "/oak:index/fooB-34"),
                List.of("/oak:index/foo"), List.of("/oak:index/fooA-34", "/oak:index/fooB-34"));
        doFilterEnabledIndexesTest(List.of("/oak:index/fooA-34"),
                List.of("/oak:index/foo"), List.of("/oak:index/anotherIndex", "/oak:index/fooA-34"));
        doFilterEnabledIndexesTest(List.of(),
                List.of("/oak:index/foo"), List.of("/oak:index/anotherIndex"));
        doFilterEnabledIndexesTest(List.of("/oak:index/fooA-34"),
                List.of("/oak:index/fooA-", "/oak:index/fooB-"), List.of("/oak:index/fooA-34"));
        doFilterEnabledIndexesTest(List.of("/oak:index/fooA-34"),
                List.of("/oak:index/fooA-", "/oak:index/fooB-"), List.of("/oak:index/anotherIndex", "/oak:index/fooA-34"));
        doFilterEnabledIndexesTest(List.of(),
                List.of("/oak:index/fooA-"), List.of());
    }

    @Test
    public void testSplitAndTrim() {
        assertEquals(List.of(), splitAndTrim(""));
        assertEquals(List.of(), splitAndTrim("   "));
        assertEquals(List.of("/oak:index/fooA-34"), splitAndTrim("/oak:index/fooA-34"));
        assertEquals(List.of("/oak:index/fooA-34"), splitAndTrim("/oak:index/fooA-34,"));
        assertEquals(List.of("/oak:index/fooA", "/oak:index/fooA"), splitAndTrim("/oak:index/fooA,/oak:index/fooA"));
        assertEquals(List.of("/oak:index/fooA-34", "/oak:index/fooB-34"), splitAndTrim("/oak:index/fooA-34, /oak:index/fooB-34"));
        assertEquals(List.of("/oak:index/fooA-34", "/oak:index/fooB-34"), splitAndTrim("/oak:index/fooA-34  , /oak:index/fooB-34"));
    }

    private void doFilterEnabledIndexesTest(List<String> expectedIndexes, List<String> enabledForIndexes, List<String> indexNames) {
        List<TestNodeStateIndexer> indexers = createIndexersWithName(indexNames);
        List<TestNodeStateIndexer> enabledIndexers = filterEnabledIndexes(enabledForIndexes, indexers);
        assertEquals(
                Set.copyOf(expectedIndexes),
                enabledIndexers.stream().map(NodeStateIndexer::getIndexName).collect(Collectors.toSet())
        );
    }

    private List<TestNodeStateIndexer> createIndexersWithName(List<String> indexNames) {
        return indexNames.stream()
                .map(name -> new TestNodeStateIndexer(name, List.of()))
                .collect(Collectors.toList());
    }
}