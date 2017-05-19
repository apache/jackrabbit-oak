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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexInfo;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneIndexInfoProviderTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    private NodeStore store = new MemoryNodeStore();
    private AsyncIndexInfoService asyncService = mock(AsyncIndexInfoService.class);
    private LuceneIndexInfoProvider provider;

    @Before
    public void setUp() {
        provider = new LuceneIndexInfoProvider(store, asyncService, temporaryFolder.getRoot());
    }

    @Test(expected = IllegalArgumentException.class)
    public void infoNonExisting() throws Exception {
        provider.getInfo("/no/existing/path");
    }

    @Test
    public void info() throws Exception {
        IndexDefinitionBuilder defnBuilder = new IndexDefinitionBuilder();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index").setChildNode("fooIndex", defnBuilder.build());
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        when(asyncService.getInfo("async"))
                .thenReturn(new AsyncIndexInfo("async", 0,0, false, null));

        IndexInfo info = provider.getInfo("/oak:index/fooIndex");

        assertNotNull(info);
    }

}