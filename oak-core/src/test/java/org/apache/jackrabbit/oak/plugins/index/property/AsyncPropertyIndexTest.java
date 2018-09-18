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
package org.apache.jackrabbit.oak.plugins.index.property;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_REINDEX_VALUE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Test the asynchronous reindexing ability of an synchronous index
 */
public class AsyncPropertyIndexTest {

    private IndexEditorProvider provider = new PropertyIndexEditorProvider();

    private EditorHook hook = new EditorHook(new IndexUpdateProvider(provider));

    @Test
    public void testAsyncPropertyLookup() throws Exception {
        NodeStore store = new MemoryNodeStore();
        assertTrue(Iterables.isEmpty(store.checkpoints()));

        NodeBuilder builder = store.getRoot().builder();

        //add a property index on 'foo'
        NodeBuilder def = createIndexDefinition(
                builder.child(INDEX_DEFINITIONS_NAME), "foo", true, false,
                of("foo"), null);
        def.setProperty(REINDEX_ASYNC_PROPERTY_NAME, true);

        // add some content
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                STRINGS);
        NodeState head = store.merge(builder, hook, EMPTY);

        // query the index, check it doesn't get indexed by the normal PI
        FilterImpl f = createFilter(head, NT_BASE);
        PropertyIndexLookup lookup = new PropertyIndexLookup(head);
        try {
            assertEquals(of(), find(lookup, "foo", "abc", f));
            fail();
        } catch (IllegalArgumentException e) {
            // expected: no index for "foo"
        }

        // run async first time, there are some changes
        AsyncIndexUpdate async = new AsyncIndexUpdate(ASYNC_REINDEX_VALUE,
                store, provider, true);
        async.run();
        assertEquals(ASYNC_REINDEX_VALUE,
                store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME)
                        .getChildNode("foo").getString(ASYNC_PROPERTY_NAME));

        // run async second time, there are no changes, should switch to sync
        async.run();
        async.close();
        assertEquals(null, store.getRoot().getChildNode(INDEX_DEFINITIONS_NAME)
                .getChildNode("foo").getString(ASYNC_PROPERTY_NAME));
        assertTrue(Iterables.isEmpty(store.checkpoints()));

        // add content, it should be indexed synchronously
        builder = store.getRoot().builder();
        builder.child("c").setProperty("foo", "def");
        head = store.merge(builder, hook, EMPTY);
        f = createFilter(head, NT_BASE);
        lookup = new PropertyIndexLookup(head);
        assertEquals(ImmutableSet.of("b", "c"), find(lookup, "foo", "def", f));
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]",
                new QueryEngineSettings());
    }

    private static Set<String> find(PropertyIndexLookup lookup, String name,
            String value, Filter filter) {
        return Sets.newHashSet(lookup.query(filter, name, value == null ? null
                : PropertyValues.newString(value)));
    }

    @Test
    public void testAsyncPropertyNoChanges() throws Exception {
        NodeStore store = new MemoryNodeStore();
        assertTrue(Iterables.isEmpty(store.checkpoints()));
        AsyncIndexUpdate async = new AsyncIndexUpdate(ASYNC_REINDEX_VALUE,
                store, provider, true);
        async.run();
        async.run();
        async.close();
        assertTrue(Iterables.isEmpty(store.checkpoints()));
    }
}
