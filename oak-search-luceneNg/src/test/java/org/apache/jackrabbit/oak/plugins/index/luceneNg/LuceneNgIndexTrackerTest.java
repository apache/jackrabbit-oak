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

import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.*;

public class LuceneNgIndexTrackerTest {

    private NodeState root;
    private NodeBuilder builder;

    @Before
    public void setup() {
        root = INITIAL_CONTENT;
        builder = root.builder();

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
        tracker.update(builder.getNodeState());
        // Should not throw
    }

    @Test
    public void testGetIndexNode() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());

        assertNotNull(tracker.acquireIndexNode("/oak:index/testIndex"));
    }

    @Test
    public void testGetNonExistentIndex() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());

        assertNull(tracker.acquireIndexNode("/oak:index/nonexistent"));
    }

    @Test
    public void testIndexRemovedOnNextUpdate() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());
        assertNotNull("Index should be tracked initially",
                tracker.acquireIndexNode("/oak:index/testIndex"));

        // Remove the index definition
        builder.child("oak:index").getChildNode("testIndex").remove();
        tracker.update(builder.getNodeState());

        assertNull("Index should no longer be tracked after removal",
                tracker.acquireIndexNode("/oak:index/testIndex"));
    }

    @Test
    public void testActiveTargetFlip_StopsTracking() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());
        assertNotNull(tracker.acquireIndexNode("/oak:index/testIndex"));

        // Flip activeTarget away from lucene9
        NodeBuilder idx = builder.child("oak:index").child("testIndex");
        idx.removeProperty("type");
        idx.setProperty("type", "lucene47");
        idx.setProperty(PropertyStates.createProperty(
                "storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS));
        idx.setProperty("activeTarget", "lucene47");

        tracker.update(builder.getNodeState());

        assertNull("Index with activeTarget=lucene47 should not be tracked",
                tracker.acquireIndexNode("/oak:index/testIndex"));
    }

    @Test
    public void testActiveTargetFlip_StartsTracking() {
        // Start with lucene47 active
        NodeBuilder idx = builder.child("oak:index").child("testIndex");
        idx.removeProperty("type");
        idx.setProperty("type", "lucene47");
        idx.setProperty(PropertyStates.createProperty(
                "storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS));
        idx.setProperty("activeTarget", "lucene47");

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());
        assertNull("Index with activeTarget=lucene47 should not be tracked initially",
                tracker.acquireIndexNode("/oak:index/testIndex"));

        // Flip to lucene9
        idx.setProperty("activeTarget", "lucene9");
        tracker.update(builder.getNodeState());

        assertNotNull("Index with activeTarget=lucene9 should now be tracked",
                tracker.acquireIndexNode("/oak:index/testIndex"));
    }

    @Test
    public void testOnlyLucene9IndexesTracked() {
        builder.child("oak:index").child("legacyIndex")
                .setProperty("type", "lucene");

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(builder.getNodeState());

        assertNotNull(tracker.acquireIndexNode("/oak:index/testIndex"));
        assertNull("Legacy lucene index should not be tracked",
                tracker.acquireIndexNode("/oak:index/legacyIndex"));
    }
}
