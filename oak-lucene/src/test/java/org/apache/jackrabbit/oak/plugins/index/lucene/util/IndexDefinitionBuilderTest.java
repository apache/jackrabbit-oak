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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.util.Iterator;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.After;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.AGGREGATES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

public class IndexDefinitionBuilderTest {
    private IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    private NodeBuilder nodeBuilder = EMPTY_NODE.builder();

    @After
    public void dumpState(){
        System.out.println(NodeStateUtils.toString(builder.build()));
    }

    @Test
    public void defaultSetup() throws Exception{
        NodeState state = builder.build();
        assertEquals(2, state.getLong("compatVersion"));
        assertEquals("async", state.getString("async"));
        assertEquals("lucene", state.getString("type"));
    }

    @Test
    public void indexRule() throws Exception{
        builder.includedPaths("/a", "/b");
        builder.queryPaths("/c", "/d");
        builder.supersedes("/e", "/f");
        builder.indexRule("nt:base")
                    .property("foo")
                        .ordered()
                .enclosingRule()
                    .property("bar")
                        .analyzed()
                        .propertyIndex()
                .enclosingRule()
                    .property("baz")
                    .propertyIndex();

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        assertTrue(state.getChildNode("indexRules").getChildNode("nt:base").exists());
        assertEquals(asList("/a", "/b"), state.getProperty(PathFilter.PROP_INCLUDED_PATHS).getValue(Type.STRINGS));
        assertEquals(asList("/c", "/d"), state.getProperty(IndexConstants.QUERY_PATHS).getValue(Type.STRINGS));
        assertEquals(asList("/e", "/f"), state.getProperty(IndexConstants.SUPERSEDED_INDEX_PATHS).getValue(Type.STRINGS));
    }

    @Test
    public void propertyDefIndexPropertySetIndexFalse() throws Exception {
        builder.indexRule("nt:base")
                .property("foo")
                .disable();

        PropertyState state = builder.build().
                getChildNode("indexRules").
                getChildNode("nt:base").
                getChildNode("properties").
                getChildNode("foo").
                getProperty("index");

        assertNotNull("index property must exist", state);
        assertFalse("Incorrect default value of index property", state.getValue(Type.BOOLEAN));
    }

    @Test
    public void aggregates() throws Exception{
        builder.aggregateRule("cq:Page").include("jcr:content").relativeNode();
        builder.aggregateRule("dam:Asset", "*", "*/*");

        NodeState state = builder.build();
        assertTrue(state.getChildNode("aggregates").exists());
        assertTrue(state.getChildNode("aggregates").getChildNode("dam:Asset").exists());
        assertTrue(state.getChildNode("aggregates").getChildNode("cq:Page").exists());
    }

    @Test
    public void duplicatePropertyName() throws Exception{
        builder.indexRule("nt:base")
                    .property("foo")
                        .ordered()
                .enclosingRule()
                    .property("jcr:content/foo")
                        .analyzed()
                        .propertyIndex()
                .enclosingRule()
                    .property("metadata/content/foo")
                        .propertyIndex();

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        assertTrue(state.getChildNode("indexRules").getChildNode("nt:base").exists());
        assertEquals(3, state.getChildNode("indexRules").getChildNode("nt:base")
                .getChildNode("properties").getChildNodeCount(10));
    }

    @Test
    public void ruleOrder() throws Exception{
        builder.indexRule("nt:unstructured");
        builder.indexRule("nt:base");

        Tree tree = TreeFactory.createTree(EMPTY_NODE.builder());
        builder.build(tree);

        //Assert the order
        Iterator<Tree> children = tree.getChild("indexRules").getChildren().iterator();
        assertEquals("nt:unstructured", children.next().getName());
        assertEquals("nt:base", children.next().getName());
    }

    @Test
    public void regexProperty() throws Exception{
        builder.indexRule("nt:base")
                .property(LuceneIndexConstants.REGEX_ALL_PROPS, true);

        NodeState state = builder.build();
        assertTrue(NodeStateUtils.getNode(state, "indexRules/nt:base/properties/prop")
                .getBoolean(LuceneIndexConstants.PROP_IS_REGEX));
    }

    @Test
    public void mergeExisting() throws Exception{
        nodeBuilder.setProperty("foo", "bar");
        builder = new IndexDefinitionBuilder(nodeBuilder);

        NodeState state = builder.build();
        assertEquals("bar", state.getString("foo"));
        assertEquals("async", state.getString("async"));
    }

    @Test
    public void mergeExisting_IndexRule() throws Exception{
        builder.indexRule("nt:unstructured").property("foo").propertyIndex();

        nodeBuilder = builder.build().builder();
        builder = new IndexDefinitionBuilder(nodeBuilder);

        assertTrue(builder.hasIndexRule("nt:unstructured"));
        assertFalse(builder.hasIndexRule("nt:base"));

        builder.indexRule("nt:unstructured").property("bar").propertyIndex();
        builder.indexRule("nt:base");

        assertTrue(builder.indexRule("nt:unstructured").hasPropertyRule("foo"));
        assertTrue(builder.indexRule("nt:unstructured").hasPropertyRule("bar"));
    }

    @Test
    public void mergeExisting_Aggregates() throws Exception{
        builder.aggregateRule("foo").include("/path1");
        builder.aggregateRule("foo").include("/path2");

        nodeBuilder = builder.build().builder();

        builder = new IndexDefinitionBuilder(nodeBuilder);

        builder.aggregateRule("foo").include("/path1");
        builder.aggregateRule("foo").include("/path3");

        NodeState state = builder.build();
        assertEquals(3, state.getChildNode(AGGREGATES).getChildNode("foo").getChildNodeCount(100));
    }

    @Test
    public void noReindexIfNoChange() throws Exception{
        builder.includedPaths("/a", "/b");
        builder.indexRule("nt:base")
                .property("foo")
                .ordered();

        nodeBuilder = builder.build().builder();
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.includedPaths("/a", "/b");

        assertFalse(builder.isReindexRequired());
        NodeState state = builder.build();
        assertFalse(state.getBoolean(REINDEX_PROPERTY_NAME));


        NodeState baseState = builder.build();
        nodeBuilder = baseState.builder();
        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.indexRule("nt:file");

        assertTrue(builder.isReindexRequired());
        state = builder.build();
        assertTrue(state.getBoolean(REINDEX_PROPERTY_NAME));

        builder = new IndexDefinitionBuilder(baseState.builder(), false);
        builder.indexRule("nt:file");
        assertTrue(builder.isReindexRequired());
        state = builder.build();
        assertTrue(builder.isReindexRequired());
        assertFalse(state.getBoolean(REINDEX_PROPERTY_NAME));
    }

    @Test
    public void reindexAndAsyncFlagChange() throws Exception{
        builder.async("async", IndexConstants.INDEXING_MODE_NRT);

        nodeBuilder = builder.build().builder();
        nodeBuilder.setProperty(REINDEX_PROPERTY_NAME, false);

        NodeState oldState = nodeBuilder.getNodeState();

        builder = new IndexDefinitionBuilder(nodeBuilder);
        builder.async("async", IndexConstants.INDEXING_MODE_SYNC);
        assertFalse(builder.build().getBoolean(REINDEX_PROPERTY_NAME));

        builder = new IndexDefinitionBuilder(oldState.builder());
        builder.async("fulltext-async", IndexConstants.INDEXING_MODE_SYNC);
        assertTrue(builder.build().getBoolean(REINDEX_PROPERTY_NAME));
    }

    @Test
    public void propRuleCustomName() throws Exception{
        builder.indexRule("nt:base").property("foo").property("bar");
        builder.indexRule("nt:base").property("fooProp", "foo2");
        builder.indexRule("nt:base").property("fooProp", "foo2");

        Root idx = new ImmutableRoot(builder.build());
        assertTrue(idx.getTree("/indexRules/nt:base/properties/fooProp").exists());
        assertTrue(idx.getTree("/indexRules/nt:base/properties/bar").exists());
        assertTrue(idx.getTree("/indexRules/nt:base/properties/foo").exists());
    }

    @Test
    public void typeNotChangedIfSet() throws Exception{
        NodeState state = builder.build();
        assertEquals("lucene", state.getString("type"));

        NodeBuilder updated = state.builder();
        updated.setProperty("type", "disabled");
        IndexDefinitionBuilder newBuilder = new IndexDefinitionBuilder(updated);

        NodeState updatedState = newBuilder.build();
        assertEquals("disabled", updatedState.getString("type"));

        //Type other than 'disabled' would be reset
        updated.setProperty("type", "foo");
        assertEquals("lucene", new IndexDefinitionBuilder(updated).build().getString("type"));
    }

    @Test
    public void nodeTypeIndex() throws Exception{
        builder.nodeTypeIndex();
        builder.indexRule("nt:file");

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        NodeState ntFileRule = state.getChildNode("indexRules").getChildNode("nt:file");
        assertTrue(ntFileRule.exists());
        assertTrue(state.getBoolean(LuceneIndexConstants.PROP_INDEX_NODE_TYPE));
        assertFalse(ntFileRule.getBoolean(LuceneIndexConstants.PROP_SYNC));
    }

    @Test
    public void nodeTypeIndexSync() throws Exception{
        builder.nodeTypeIndex();
        builder.indexRule("nt:file").sync();

        NodeState state = builder.build();
        assertTrue(state.getChildNode("indexRules").exists());
        NodeState ntFileRule = state.getChildNode("indexRules").getChildNode("nt:file");
        assertTrue(ntFileRule.exists());
        assertTrue(state.getBoolean(LuceneIndexConstants.PROP_INDEX_NODE_TYPE));
        assertTrue(ntFileRule.getBoolean(LuceneIndexConstants.PROP_SYNC));
    }

    @Test
    public void noPropertiesNodeForEmptyRule() throws Exception{
        builder.nodeTypeIndex();
        builder.indexRule("nt:file").sync();

        NodeState state = builder.build();
        assertFalse(NodeStateUtils.getNode(state, "/indexRules/nt:file/properties").exists());
    }
}