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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_HEAD_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_PREVIOUS_BUCKET;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BucketSwitcherTest {
    private NodeBuilder builder = EMPTY_NODE.builder();
    private BucketSwitcher bs = new BucketSwitcher(builder);

    @Test
    public void basic() throws Exception {
        bs.switchBucket(100);
        assertThat(copyOf(bs.getOldBuckets()), empty());
    }

    @Test
    public void singleUnusedBucket() throws Exception {
        builder.child("1");
        builder.setProperty(PROP_HEAD_BUCKET, "1");

        bs.switchBucket(100);
        assertThat(copyOf(bs.getOldBuckets()), empty());
    }

    @Test
    public void twoBucket_HeadUnused() throws Exception {
        builder.child("1");
        builder.child("2");
        builder.setProperty(PROP_HEAD_BUCKET, "2");
        builder.setProperty(PROP_PREVIOUS_BUCKET, "1");

        bs.switchBucket(100);
        assertFalse(builder.hasProperty(PROP_PREVIOUS_BUCKET));
        assertEquals("2", builder.getString(PROP_HEAD_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1"));
    }

    @Test
    public void twoBuckets_BothUsed() throws Exception {
        builder.child("2").child("foo");
        builder.child("1");
        builder.setProperty(PROP_HEAD_BUCKET, "2");
        builder.setProperty(PROP_PREVIOUS_BUCKET, "1");

        bs.switchBucket(100);

        assertEquals("2", builder.getString(PROP_PREVIOUS_BUCKET));
        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertTrue(builder.hasChildNode("3"));

        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1"));
    }

    @Test
    public void twoBuckets_2Switches() throws Exception{
        builder.child("2").child("foo");
        builder.child("1");
        builder.setProperty(PROP_HEAD_BUCKET, "2");
        builder.setProperty(PROP_PREVIOUS_BUCKET, "1");

        bs.switchBucket(100);

        bs.switchBucket(150);
        assertFalse(builder.hasProperty(PROP_PREVIOUS_BUCKET));
        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1", "2"));
    }

    /**
     * Test the case where lastIndexedTo time does not change i.e. async indexer
     * has not moved on between different run. In such a case the property index
     * state should remain same and both head and previous bucket should not be
     * changed
     */
    @Test
    public void twoBuckets_NoChange() throws Exception{
        builder.child("2").child("foo");
        builder.child("1");
        builder.setProperty(PROP_HEAD_BUCKET, "2");
        builder.setProperty(PROP_PREVIOUS_BUCKET, "1");

        NodeState state0 = builder.getNodeState();
        assertTrue(bs.switchBucket(100));

        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertEquals("2", builder.getString(PROP_PREVIOUS_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1"));

        NodeState state1 = builder.getNodeState();
        assertFalse(bs.switchBucket(100));

        //No changed in async indexer state so current bucket state would remain
        //as previous
        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertEquals("2", builder.getString(PROP_PREVIOUS_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1"));

        NodeState state2 = builder.getNodeState();
        assertFalse(bs.switchBucket(100));

        //Async indexer time still not changed. So head bucket remains same
        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertEquals("2", builder.getString(PROP_PREVIOUS_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1"));

        NodeState state3 = builder.getNodeState();

        assertTrue(EqualsDiff.equals(state1, state2));
        assertTrue(EqualsDiff.equals(state2, state3));
    }

    /**
     * Test the case where async indexer state changes i.e. lastIndexedTo changes
     * however nothing new got indexed in property index. In such a case
     * after the second run there should be no previous bucket
     */
    @Test
    public void twoBucket_IndexedToTimeChange() throws Exception{
        builder.child("2").child("foo");
        builder.child("1");
        builder.setProperty(PROP_HEAD_BUCKET, "2");
        builder.setProperty(PROP_PREVIOUS_BUCKET, "1");

        NodeState state0 = builder.getNodeState();
        assertTrue(bs.switchBucket(100));

        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertEquals("2", builder.getString(PROP_PREVIOUS_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1"));

        NodeState state1 = builder.getNodeState();
        assertTrue(bs.switchBucket(150));

        //This time previous bucket should be discarded
        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertNull(builder.getString(PROP_PREVIOUS_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1", "2"));

        NodeState state2 = builder.getNodeState();
        assertTrue(bs.switchBucket(200));

        assertEquals("3", builder.getString(PROP_HEAD_BUCKET));
        assertNull(builder.getString(PROP_PREVIOUS_BUCKET));
        assertThat(copyOf(bs.getOldBuckets()), containsInAnyOrder("1", "2"));

        //assert no change done after previous is removed
        //not even change of asyncIndexedTo
        NodeState state3 = builder.getNodeState();
        assertTrue(EqualsDiff.equals(state2, state3));
    }

}