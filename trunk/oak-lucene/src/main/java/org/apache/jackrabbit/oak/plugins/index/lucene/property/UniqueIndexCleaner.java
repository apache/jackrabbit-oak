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

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.lucene.property.HybridPropertyIndexUtil.PROP_CREATED;

class UniqueIndexCleaner {
    private final long createTimeMarginMillis;

    public UniqueIndexCleaner(TimeUnit timeUnit, long createTimeMargin) {
        this.createTimeMarginMillis = timeUnit.toMillis(createTimeMargin);
    }

    public int clean(NodeBuilder builder, long lastIndexedTo) {
        int removalCount = 0;
        NodeState baseState = builder.getBaseState();
        for (ChildNodeEntry e : baseState.getChildNodeEntries()) {
            long entryCreationTime = e.getNodeState().getLong(PROP_CREATED);
            if (entryCovered(entryCreationTime, lastIndexedTo)) {
                builder.child(e.getName()).remove();
                removalCount++;
            }
        }
        return removalCount;
    }

    private boolean entryCovered(long entryCreationTime, long lastIndexedTo) {
        //Would be safer to add some margin as entryCreationTime as recorded
        //is not same as actual commit time
        return (lastIndexedTo - entryCreationTime) >= createTimeMarginMillis;
    }
}
