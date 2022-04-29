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
package org.apache.jackrabbit.oak.plugins.index.search.spi.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.slf4j.event.Level;
import java.util.ArrayList;

/**
 * Test the IndexName class
 */
public class IndexNameTest {

    @Test
    public void parse() {
        assertEquals("/lucene base=/lucene product=0 custom=0",
                IndexName.parse("/lucene").toString());
        assertEquals("/lucene-1 base=/lucene versioned product=1 custom=0",
                IndexName.parse("/lucene-1").toString());
        assertEquals("/lucene-2 base=/lucene versioned product=2 custom=0",
                IndexName.parse("/lucene-2").toString());
        assertEquals("/lucene-custom-3 base=/lucene versioned product=0 custom=3",
                IndexName.parse("/lucene-custom-3").toString());
        assertEquals("/lucene-4-custom-5 base=/lucene versioned product=4 custom=5",
                IndexName.parse("/lucene-4-custom-5").toString());
        assertEquals("/lucene-12-custom-34 base=/lucene versioned product=12 custom=34",
                IndexName.parse("/lucene-12-custom-34").toString());
        // illegal
        assertEquals("/lucene-abc base=/lucene-abc product=0 custom=0 illegal",
                IndexName.parse("/lucene-abc").toString());
        assertEquals("/lucene-abc-custom-2 base=/lucene-abc-custom-2 product=0 custom=0 illegal",
                IndexName.parse("/lucene-abc-custom-2").toString());
    }

    @Test
    public void compare() {
        IndexName p0 = IndexName.parse("/lucene");
        IndexName p0a = IndexName.parse("/lucene-0");
        IndexName p0b = IndexName.parse("/lucene-0-custom-0");
        IndexName p0c1 = IndexName.parse("/lucene-custom-1");
        IndexName p0c1a = IndexName.parse("/lucene-0-custom-1");
        IndexName p1 = IndexName.parse("/lucene-1");
        IndexName p1c1 = IndexName.parse("/lucene-1-custom-1");
        IndexName p1c2 = IndexName.parse("/lucene-1-custom-2");

        assertTrue(p0.compareTo(p0a) == 0);
        assertTrue(p0.compareTo(p0b) == 0);
        assertTrue(p0a.compareTo(p0b) == 0);
        assertTrue(p0c1.compareTo(p0c1a) == 0);

        assertTrue(p0.compareTo(p0c1) < 0);
        assertTrue(p0c1.compareTo(p1) < 0);
        assertTrue(p1.compareTo(p1c1) < 0);
        assertTrue(p1c1.compareTo(p1c2) < 0);

        IndexName a = IndexName.parse("/luceneA");
        IndexName b = IndexName.parse("/luceneB");
        IndexName c = IndexName.parse("/luceneC");
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(c) < 0);

    }

    @Test
    public void recursiveActive() {
        LogCustomizer lc = LogCustomizer.forLogger(IndexName.class)
                .enable(Level.ERROR)
                .filter(Level.ERROR)
                .create();

        NodeBuilder builder = EMPTY_NODE.builder();
        NodeBuilder luceneOneBuilder = builder.child("lucene-1");
        NodeBuilder luceneTwoBuilder = builder.child("lucene-2");
        luceneOneBuilder.setProperty("merges", "lucene-1");
        luceneTwoBuilder.setProperty("merges", "lucene-2");
        NodeState base = builder.getNodeState();
        ArrayList<String> indexPaths = new ArrayList<>();
        indexPaths.add("lucene-1");

        try {
            lc.starting();
            IndexName.filterReplacedIndexes(indexPaths, base, true);
            assertTrue("StackOverflowError should be caught", lc.getLogs().contains("Fail to check index activeness for lucene-1 due to StackOverflowError"));
        } catch (StackOverflowError e) {
            fail("should not run into StackOverflowError exception");
        } finally {
            lc.finished();
        }
    }
}
