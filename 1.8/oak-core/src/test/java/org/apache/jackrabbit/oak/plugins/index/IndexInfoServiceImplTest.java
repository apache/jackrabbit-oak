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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexInfoServiceImplTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private IndexInfoServiceImpl service = new IndexInfoServiceImpl();

    private NodeStore store = new MemoryNodeStore();
    private SimpleIndexPathService pathService = new SimpleIndexPathService();

    @Before
    public void setUp(){
        context.registerService(NodeStore.class, store);
        context.registerService(IndexPathService.class, pathService);
        MockOsgi.injectServices(service, context.bundleContext());
    }

    @Test
    public void indexInfo() throws Exception{
        //1. Test Empty
        assertNull(service.getInfo("/nonExistingPath"));

        //2. Test when no backing InfoProvider
        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index").child("fooIndex").setProperty("type", "foo");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexInfo info = service.getInfo("/oak:index/fooIndex");
        assertNotNull(info);

        assertEquals("/oak:index/fooIndex", info.getIndexPath());
        assertEquals("foo", info.getType());

        //3. Test when backing InfoProvider
        IndexInfo testInfo = mock(IndexInfo.class);
        when(testInfo.getIndexPath()).thenReturn("/some/other/path");

        IndexInfoProvider infoProvider = mock(IndexInfoProvider.class);
        when(infoProvider.getType()).thenReturn("foo");
        when(infoProvider.getInfo(anyString())).thenReturn(testInfo);

        service.bindInfoProviders(infoProvider);

        IndexInfo info2 = service.getInfo("/oak:index/fooIndex");
        assertNotNull(info2);
        assertEquals("/some/other/path", info2.getIndexPath());
    }

    @Test
    public void allIndexInfo() throws Exception{
        pathService.paths = Lists.newArrayList("/oak:index/a", "/oak:index/b", "/oak:index/c", "/oak:index/d");

        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index").child("a"); //Index with no type
        builder.child("oak:index").child("b").setProperty("type", "type-b"); //No backing info provider
        builder.child("oak:index").child("c").setProperty("type", "type-c"); //Info provider throws exception
        builder.child("oak:index").child("d").setProperty("type", "type-d"); //Info provider returns result
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexInfoProvider type_c = mock(IndexInfoProvider.class);
        when(type_c.getInfo(anyString())).thenThrow(new RuntimeException());
        when(type_c.getType()).thenReturn("type-c");

        IndexInfo infod = mock(IndexInfo.class);
        when(infod.getType()).thenReturn("type-d");
        when(infod.getAsyncLaneName()).thenReturn("async-d");

        IndexInfoProvider type_d = mock(IndexInfoProvider.class);
        when(type_d.getInfo(anyString())).thenReturn(infod);
        when(type_d.getType()).thenReturn("type-d");

        service.bindInfoProviders(type_c);
        service.bindInfoProviders(type_d);

        List<IndexInfo> infos = Lists.newArrayList(service.getAllIndexInfo());

        //Result would only have 2 entries. One throwing exception would be ignored
        assertEquals(2, infos.size());

        for (IndexInfo info : infos) {
            if (info.getType().equals("type-d")){
                assertEquals("async-d", info.getAsyncLaneName());
            }
        }

    }

    private static class SimpleIndexPathService implements IndexPathService {
        List<String> paths = Collections.emptyList();

        @Override
        public Iterable<String> getIndexPaths() {
            return paths;
        }
    }

}