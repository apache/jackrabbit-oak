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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

public class AsyncIndexInfoServiceImplTest {

    private MemoryNodeStore store = new MemoryNodeStore();
    private PropertyIndexEditorProvider provider = new PropertyIndexEditorProvider();

    private AsyncIndexInfoServiceImpl service = new AsyncIndexInfoServiceImpl(store);

    @Test
    public void names() throws Exception{
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();

        AsyncIndexUpdate async2 = new AsyncIndexUpdate("foo-async", store, provider);
        async2.run();

        Set<String> names = ImmutableSet.copyOf(service.getAsyncLanes());
        assertThat(names, containsInAnyOrder("async", "foo-async"));

        service.bindStatsMBeans(async.getIndexStats());
        service.bindStatsMBeans(async2.getIndexStats());
    }

    @Test
    public void info() throws Exception{
        AsyncIndexUpdate async = new AsyncIndexUpdate("foo-async", store, provider);
        async.run();

        Set<String> names = ImmutableSet.copyOf(service.getAsyncLanes());
        assertThat(names, containsInAnyOrder("foo-async"));

        service.bindStatsMBeans(async.getIndexStats());

        AsyncIndexInfo info = service.getInfo("foo-async");
        assertNotNull(info);
        assertEquals("foo-async", info.getName());
        assertNotNull(info.getStatsMBean());
        assertTrue(info.getLastIndexedTo() > -1);
        assertFalse(info.isRunning());
        assertEquals(-1, info.getLeaseExpiryTime());
        System.out.println(info);

        service.unbindStatsMBeans(async.getIndexStats());

        AsyncIndexInfo info2 = service.getInfo("foo-async");
        assertNull(info2.getStatsMBean());
    }

    @Test
    public void indexedUpto() throws Exception{
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();

        AsyncIndexUpdate async2 = new AsyncIndexUpdate("foo-async", store, provider);
        async2.run();

        Map<String, Long> result = service.getIndexedUptoPerLane();

        assertFalse(result.isEmpty());
        assertTrue(result.get("async") > -1);
        assertTrue(result.get("foo-async") > -1);
    }

    @Test
    public void asyncStateChanged() throws Exception{
        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();

        AsyncIndexUpdate async2 = new AsyncIndexUpdate("foo-async", store, provider);
        async2.run();

        NodeState root = store.getRoot();
        assertFalse(service.hasIndexerUpdatedForAnyLane(root, root));

        NodeBuilder builder = store.getRoot().builder();
        builder.child(":async").setProperty(AsyncIndexUpdate.lastIndexedTo("async"), 42L);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(service.hasIndexerUpdatedForAnyLane(root, store.getRoot()));
    }

}