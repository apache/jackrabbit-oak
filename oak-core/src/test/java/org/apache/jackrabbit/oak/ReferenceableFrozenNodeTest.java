/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak;

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Test;

/**
 * Tests the implementation of mix:referenceable in nt:frozenNode.
 * By default from JCR 2.0, nt:frozenNode shouldn't implement mix:referenceable
 * However, the System Property oak.referenceableFrozenNode can be set to true to change this behaviour.
 */
public class ReferenceableFrozenNodeTest {
    
    private NodeState initializeRepository() throws CommitFailedException {
        NodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return store.getRoot();
    }
    
    @After
    public void clearSystemProperty() {
        System.clearProperty("oak.referenceableFrozenNode");
    }

    @Test
    public void testInitializeRepositoryWithoutReferenceableFrozenNode() throws CommitFailedException {
        System.setProperty("oak.referenceableFrozenNode", "false");
        doTestInitializeRepositoryWithoutReferenceableFrozenNode();
    }

    @Test
    public void testInitializeRepositoryWithoutReferenceableFrozenNode_noProperty() throws CommitFailedException {
        System.clearProperty("oak.referenceableFrozenNode");
        doTestInitializeRepositoryWithoutReferenceableFrozenNode();
    }

    private void doTestInitializeRepositoryWithoutReferenceableFrozenNode() throws CommitFailedException {
        NodeState root = initializeRepository();
        
        NodeState frozenNode = root.getChildNode("jcr:system").getChildNode("jcr:nodeTypes").getChildNode("nt:frozenNode");
        PropertyState superTypes = frozenNode.getProperty("jcr:supertypes");
        
        assertEquals(1, superTypes.count());
        assertEquals("nt:base", superTypes.getValue(Type.NAME, 0));
    }
    
    @Test
    public void testInitializeRepositoryWithReferenceableFrozenNode() throws CommitFailedException {
        System.setProperty("oak.referenceableFrozenNode", "true");
        NodeState root = initializeRepository();
        
        NodeState frozenNode = root.getChildNode("jcr:system").getChildNode("jcr:nodeTypes").getChildNode("nt:frozenNode");
        PropertyState superTypes = frozenNode.getProperty("jcr:supertypes");
        
        assertEquals(2, superTypes.count());
        assertEquals("nt:base", superTypes.getValue(Type.NAME, 0));
        assertEquals("mix:referenceable", superTypes.getValue(Type.NAME, 1));
    }
    
    @Test
    public void testReinitializeRepositoryWithProperty() throws CommitFailedException {
        System.setProperty("oak.referenceableFrozenNode", "false");
        NodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        System.setProperty("oak.referenceableFrozenNode", "true");
        new InitialContent().initialize(builder);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState root = store.getRoot();
        
        NodeState frozenNode = root.getChildNode("jcr:system").getChildNode("jcr:nodeTypes").getChildNode("nt:frozenNode");
        PropertyState superTypes = frozenNode.getProperty("jcr:supertypes");
        
        assertEquals(1, superTypes.count());
        assertEquals("nt:base", superTypes.getValue(Type.NAME, 0));
    }
    
    @Test
    public void testReinitializeRepositoryWithoutProperty() throws CommitFailedException {
        System.setProperty("oak.referenceableFrozenNode", "true");
        NodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        System.setProperty("oak.referenceableFrozenNode", "false");
        new InitialContent().initialize(builder);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState root = store.getRoot();
        
        NodeState frozenNode = root.getChildNode("jcr:system").getChildNode("jcr:nodeTypes").getChildNode("nt:frozenNode");
        PropertyState superTypes = frozenNode.getProperty("jcr:supertypes");
        
        assertEquals(2, superTypes.count());
        assertEquals("nt:base", superTypes.getValue(Type.NAME, 0));
        assertEquals("mix:referenceable", superTypes.getValue(Type.NAME, 1));
    }
}
