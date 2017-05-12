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

package org.apache.jackrabbit.oak.plugins.index.property;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PropertyIndexInfoProviderTest {
    private static final CommitHook HOOK = new EditorHook(
            new IndexUpdateProvider(new PropertyIndexEditorProvider()));

    private NodeStore store = new MemoryNodeStore();

    private PropertyIndexInfoProvider infoProvider = new PropertyIndexInfoProvider(store);

    @Before
    public void setUp() throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void emptyIndexEstimate() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        store.merge(builder, HOOK, CommitInfo.EMPTY);

        IndexInfo info = infoProvider.getInfo("/oak:index/foo");
        assertNotNull(info);

        assertEquals("/oak:index/foo", info.getIndexPath());
        assertEquals("property", info.getType());
        assertEquals(-2, info.getLastUpdatedTime());
        assertEquals(0, info.getEstimatedEntryCount());
    }
}