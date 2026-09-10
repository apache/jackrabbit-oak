/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class LuceneNgQueryIndexProviderTest {

    @Test
    public void testGetQueryIndexes() {
        NodeBuilder builder = InitialContentHelper.INITIAL_CONTENT.builder();
        NodeBuilder oakIndex = builder.child("oak:index");

        // Create Lucene 9 index
        NodeBuilder lucene9Index = oakIndex.child("test");
        lucene9Index.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Create Lucene 4.7 index (should be ignored)
        NodeBuilder lucene47Index = oakIndex.child("old");
        lucene47Index.setProperty("type", "lucene");

        NodeState root = builder.getNodeState();

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        List<? extends QueryIndex> indexes = provider.getQueryIndexes(root);

        assertNotNull("Indexes should not be null", indexes);
        assertEquals("Should return one LuceneNgIndex", 1, indexes.size());
        assertTrue("Should be LuceneNgIndex instance",
                   indexes.get(0) instanceof LuceneNgIndex);
    }

    @Test
    public void testNoIndexesWhenNoLucene9() {
        NodeState root = InitialContentHelper.INITIAL_CONTENT;

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(root);

        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        List<? extends QueryIndex> indexes = provider.getQueryIndexes(root);

        assertNotNull("Indexes should not be null", indexes);
        assertTrue("Should return empty list when no Lucene 9 indexes",
                   indexes.isEmpty());
    }
}
