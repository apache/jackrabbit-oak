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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class LuceneNgIndexTrackerTest {

    private NodeState root;
    private NodeBuilder builder;

    @Before
    public void setup() {
        root = INITIAL_CONTENT;
        builder = root.builder();

        // Create index definition
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder testIndex = oakIndex.child("testIndex");
        testIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        testIndex.setProperty("async", "async");
    }

    @Test
    public void testTrackerCreation() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        assertNotNull(tracker);
    }

    @Test
    public void testUpdate() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        NodeState after = builder.getNodeState();

        tracker.update(after);
        // Should not throw exception
    }

    @Test
    public void testGetIndexNode() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        NodeState after = builder.getNodeState();
        tracker.update(after);

        LuceneNgIndexNode indexNode = tracker.acquireIndexNode("/oak:index/testIndex");
        assertNotNull(indexNode);
    }

    @Test
    public void testGetNonExistentIndex() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        NodeState after = builder.getNodeState();
        tracker.update(after);

        LuceneNgIndexNode indexNode = tracker.acquireIndexNode("/oak:index/nonexistent");
        assertNull(indexNode);
    }
}
