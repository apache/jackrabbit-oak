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

package org.apache.jackrabbit.oak.plugins.index.importer;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock.LockToken;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.*;

public class ClusterNodeStoreLockTest {
    private NodeStore store = new MemoryNodeStore();
    private IndexEditorProvider provider;
    private String name = "async";

    @Before
    public void setup() throws Exception {
        provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, name);
        builder.child("testRoot").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void locking() throws Exception{
        new AsyncIndexUpdate(name, store, provider).run();

        assertFalse(getAsync().hasProperty(AsyncIndexUpdate.leasify(name)));

        ClusterNodeStoreLock lock = new ClusterNodeStoreLock(store);
        ClusteredLockToken token = lock.lock("async");

        assertTrue(getAsync().hasProperty(AsyncIndexUpdate.leasify(name)));
        assertTrue(lock.isLocked(name));

        lock.unlock(token);
        assertFalse(getAsync().hasProperty(AsyncIndexUpdate.leasify(name)));
        assertFalse(lock.isLocked(name));
    }

    private NodeState getAsync() {
        return store.getRoot().getChildNode(":async");
    }

    //TODO Test for check if changing lease actually cause current running indexer to fail
}