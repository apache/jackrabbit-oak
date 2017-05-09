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

import org.apache.jackrabbit.oak.plugins.index.lucene.NodeStateCloner;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

public class NodeStateClonerTest {

    @Test
    public void simple() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").child("b");
        builder.child("a").setProperty("foo", "bar");

        NodeState state = builder.getNodeState();
        assertTrue(EqualsDiff.equals(state, NodeStateCloner.cloneVisibleState(state)));
    }

    @Test
    public void excludeHidden() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child("a").child(":b").child("e");
        builder.child("a").child("c").child(":d");
        builder.child("a").setProperty("foo", "bar");

        NodeState source = builder.getNodeState();
        NodeState cloned = NodeStateCloner.cloneVisibleState(source);

        assertFalse(NodeStateUtils.getNode(cloned, "/a/:b").exists());
        assertFalse(NodeStateUtils.getNode(cloned, "/a/:b/e").exists());
        assertFalse(NodeStateUtils.getNode(cloned, "/a/c/:d").exists());

        assertTrue(NodeStateUtils.getNode(cloned, "/a/c").exists());
        assertTrue(NodeStateUtils.getNode(cloned, "/a").hasProperty("foo"));

        assertFalse(EqualsDiff.equals(source, cloned));
    }

}