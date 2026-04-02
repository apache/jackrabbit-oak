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
package org.apache.jackrabbit.oak.plugins.index;

import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexNameAdditionalTest {

    // ---- filterGloballySuperseded ----

    @Test
    public void filterGloballySuperseded_noCompetitors() {
        // no competing paths: all candidates pass through
        Collection<String> result = IndexName.filterGloballySuperseded(
                List.of("/oak:index/lucene-2"),
                List.of());
        assertEquals(List.of("/oak:index/lucene-2"), List.copyOf(result));
    }

    @Test
    public void filterGloballySuperseded_olderCompetitorKept() {
        // lucene-2 is newer than /oak:index/lucene-1-custom-1, so it passes
        Collection<String> result = IndexName.filterGloballySuperseded(
                List.of("/oak:index/lucene-2"),
                List.of("/oak:index/lucene-2", "/oak:index/lucene-1-custom-1"));
        assertEquals(List.of("/oak:index/lucene-2"), List.copyOf(result));
    }

    @Test
    public void filterGloballySuperseded_newerCompetitorFilters() {
        // lucene-1 vs. lucene-2 (same base): lucene-1 is superseded
        Collection<String> result = IndexName.filterGloballySuperseded(
                List.of("/oak:index/lucene-1"),
                List.of("/oak:index/lucene-1", "/oak:index/lucene-2"));
        assertTrue(result.isEmpty());
    }

    @Test
    public void filterGloballySuperseded_differentBaseNotAffected() {
        // lucene-1 for "fooIndex" is not affected by a newer version of "barIndex"
        Collection<String> result = IndexName.filterGloballySuperseded(
                List.of("/oak:index/fooIndex-1"),
                List.of("/oak:index/fooIndex-1", "/oak:index/barIndex-2"));
        assertEquals(List.of("/oak:index/fooIndex-1"), List.copyOf(result));
    }

    @Test
    public void filterGloballySuperseded_unversionedSupersededByVersioned() {
        // unversioned lucene (version 0) is superseded by lucene-1
        Collection<String> result = IndexName.filterGloballySuperseded(
                List.of("/oak:index/lucene"),
                List.of("/oak:index/lucene", "/oak:index/lucene-1"));
        assertTrue(result.isEmpty());
    }
}
