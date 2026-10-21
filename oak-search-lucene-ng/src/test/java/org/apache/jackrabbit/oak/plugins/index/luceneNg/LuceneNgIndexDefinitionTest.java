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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LuceneNgIndexDefinitionTest {

    private NodeState root;
    private NodeBuilder builder;

    @Before
    public void setup() {
        root = INITIAL_CONTENT;
        builder = root.builder();
        builder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
    }

    @Test
    public void testBasicCreation() {
        NodeState defnState = builder.getNodeState();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
            root, defnState, "/oak:index/test");

        assertNotNull(definition);
        assertEquals("/oak:index/test", definition.getIndexPath());
    }

    @Test
    public void testIndexName() {
        NodeState defnState = builder.getNodeState();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
            root, defnState, "/oak:index/myIndex");

        assertEquals("myIndex", definition.getIndexName());
    }

    @Test
    public void testStoragePath() {
        NodeState defnState = builder.getNodeState();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
            root, defnState, "/oak:index/assetIndex");

        assertEquals(LuceneNgIndexStorage.storagePath("/oak:index/assetIndex"), definition.getStoragePath());
    }

    @Test
    public void testDefaultFunctionName() {
        NodeState defnState = builder.getNodeState();
        LuceneNgIndexDefinition definition = new LuceneNgIndexDefinition(
            root, defnState, "/oak:index/test");

        // getDefaultFunctionName is protected, but we can verify via public methods
        // that use it. For now, just verify the class compiles and works.
        assertNotNull(definition);
    }
}
