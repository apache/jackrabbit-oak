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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.After;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

public class IndexDefinitionBuilderTest {
    private IndexDefinitionBuilder builder = new IndexDefinitionBuilder();

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
}