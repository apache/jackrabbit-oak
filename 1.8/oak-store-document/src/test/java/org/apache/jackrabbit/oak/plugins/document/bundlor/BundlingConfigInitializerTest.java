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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class BundlingConfigInitializerTest {
    private NodeState root = INITIAL_CONTENT;

    @Test
    public void bootstrapDefault() throws Exception{
        NodeBuilder builder = root.builder();
        BundlingConfigInitializer.INSTANCE.initialize(builder);

        NodeState state = builder.getNodeState();
        NodeState bundlor = NodeStateUtils.getNode(state, BundlingConfigHandler.CONFIG_PATH);
        assertTrue(bundlor.exists());
        assertTrue(bundlor.getChildNode("nt:file").exists());
    }

    @Test
    public void noInitWhenJcrSystemNotPresent() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        BundlingConfigInitializer.INSTANCE.initialize(builder);

        NodeState state = builder.getNodeState();
        NodeState bundlor = NodeStateUtils.getNode(state, BundlingConfigHandler.CONFIG_PATH);
        assertFalse(bundlor.exists());
    }
}