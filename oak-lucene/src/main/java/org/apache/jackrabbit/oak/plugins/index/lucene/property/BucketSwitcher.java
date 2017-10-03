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

import java.util.Objects;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_HEAD_BUCKET;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_ASYNC_INDEXED_TO_TIME_AT_SWITCH;
import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_PREVIOUS_BUCKET;

/**
 * Takes care of switching the buckets used by non unique property indexes
 * based on the associated async indexer lastIndexedTo time.
 *
 * It also ensures that unnecessary changes are not done if the property index
 * does not get updated
 */
class BucketSwitcher {
    private final NodeBuilder builder;

    public BucketSwitcher(NodeBuilder builder) {
        this.builder = builder;
    }

    public boolean switchBucket(long lastIndexedTo) {
        String head = builder.getString(PROP_HEAD_BUCKET);

        if (head == null) {
            //Cleaner ran before any updates to index
            //nothing to do further
            return false;
        }

        NodeBuilder headb = builder.getChildNode(head);
        long headLastIndexedTo = getOptionalValue(headb, PROP_ASYNC_INDEXED_TO_TIME_AT_SWITCH, 0);

        if (headLastIndexedTo > lastIndexedTo) {
            //> Should not happen in general as it means that
            //async indexer clock switched back for some reason
            return false;
        }

        if (headLastIndexedTo == lastIndexedTo) {
            //Async indexer has yet not moved so keep current state
            return false;
        }

        if (asyncIndexedToTimeSameAsPrevious(lastIndexedTo)) {
            //Async indexer has yet not moved so keep current state
            return false;
        }

        if (headb.getChildNodeCount(1) > 0) {
            //Bucket non empty case
            //Create new head bucket and switch previous to current head
            String nextHeadName = String.valueOf(Integer.parseInt(head) + 1);
            builder.child(nextHeadName);
            builder.setProperty(PROP_HEAD_BUCKET, nextHeadName);
            builder.setProperty(PROP_PREVIOUS_BUCKET, head);
            headb.setProperty(PROP_ASYNC_INDEXED_TO_TIME_AT_SWITCH, lastIndexedTo);
        } else {
            //Bucket remains empty
            //Avoid unnecessary new bucket creation or any other changes
            if (headLastIndexedTo == 0) {
                //Only update time if not already set
                headb.setProperty(PROP_ASYNC_INDEXED_TO_TIME_AT_SWITCH, lastIndexedTo);
            }
            //Remove any previous bucket reference
            builder.removeProperty(PROP_PREVIOUS_BUCKET);
        }

        return builder.isModified();
    }

    public Iterable<String> getOldBuckets() {
        String head = builder.getString(PROP_HEAD_BUCKET);
        String previous = builder.getString(PROP_PREVIOUS_BUCKET);
        return Iterables.filter(builder.getChildNodeNames(),
                name -> !Objects.equals(name, head) && !Objects.equals(name, previous)
        );
    }

    private boolean asyncIndexedToTimeSameAsPrevious(long lastIndexedTo) {
        String previous = builder.getString(PROP_PREVIOUS_BUCKET);
        if (previous != null) {
            long previousAsyncIndexedTo = getOptionalValue(builder.getChildNode(previous),
                    PROP_ASYNC_INDEXED_TO_TIME_AT_SWITCH, 0);
            return previousAsyncIndexedTo == lastIndexedTo;
        }
        return false;
    }

    private static long getOptionalValue(NodeBuilder nb, String propName, int defaultVal){
        PropertyState ps = nb.getProperty(propName);
        return ps == null ? defaultVal : ps.getValue(Type.LONG);
    }
}
