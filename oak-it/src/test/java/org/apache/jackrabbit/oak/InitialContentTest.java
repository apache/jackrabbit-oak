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

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.version.VersionHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.junit.Test;

/**
 * Test for OAK-2459.
 */
public class InitialContentTest implements VersionConstants {

    @Test
    public void noVersionStoragePrePopulated() throws Exception {
        // default initial content does not have intermediate nodes
        // pre-populated
        NodeState system = INITIAL_CONTENT.getChildNode(JCR_SYSTEM);
        assertTrue(system.exists());
        
        NodeState vs = system.getChildNode(JCR_VERSIONSTORAGE);
        assertTrue(vs.exists());
        
        assertTrue(vs.getChildNodeCount(Integer.MAX_VALUE) == 0);
    }

    @Test
    public void versionStoragePrePopulated() throws Exception {
        NodeBuilder root = EMPTY_NODE.builder();
        new InitialContent().withPrePopulatedVersionStore().initialize(root);
        
        NodeBuilder system = root.getChildNode(JCR_SYSTEM);
        assertTrue(system.exists());

        NodeBuilder vs = system.getChildNode(JCR_VERSIONSTORAGE);
        assertTrue(vs.exists());

        // check if two levels of intermediate nodes were created
        assertTrue(vs.getChildNodeCount(Integer.MAX_VALUE) == 0xff);
        for (String name : vs.getChildNodeNames()) {
            assertTrue(vs.child(name).getChildNodeCount(Integer.MAX_VALUE) == 0xff);
        }
    }

    @Test
    public void bundlingConfig() throws Exception {
        NodeState system = INITIAL_CONTENT.getChildNode(JCR_SYSTEM);
        assertFalse(system.getChildNode(DOCUMENT_NODE_STORE).exists());
    }

    @Test
    public void validatePrePopulated() throws Exception {
        NodeState before = EMPTY_NODE;
        NodeBuilder builder = before.builder();
        new InitialContent().withPrePopulatedVersionStore().initialize(builder);
        NodeState after = builder.getNodeState();
        new VersionHook().processCommit(before, after, CommitInfo.EMPTY);
    }


    @Test
    public void validatePrePopulatedNonEmpty() throws Exception {
        NodeState init = EMPTY_NODE;
        NodeBuilder builderI = init.builder();

        // create a partial version storage structure
        new InitialContent().withPrePopulatedVersionStore().initialize(builderI);
        NodeBuilder versionStorage = builderI.child(JCR_SYSTEM).child(JCR_VERSIONSTORAGE);
        versionStorage.removeProperty(VERSION_STORE_INIT);
        versionStorage.getChildNode("00").getChildNode("00").remove();
        versionStorage.getChildNode("01").getChildNode("00").remove();
        versionStorage.getChildNode("02").remove();

        NodeState before = builderI.getNodeState();
        NodeBuilder builder = before.builder();
        new InitialContent().withPrePopulatedVersionStore().initialize(builder);
        NodeState after = builder.getNodeState();
        new VersionHook().processCommit(before, after, CommitInfo.EMPTY);
    }
}
