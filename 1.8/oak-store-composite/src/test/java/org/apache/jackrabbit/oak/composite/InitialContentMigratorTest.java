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
package org.apache.jackrabbit.oak.composite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class InitialContentMigratorTest {

    @Test
    public void migrateContentWithCheckpoints() throws IOException, CommitFailedException {
        
        // 1. populate the seed store with
        // .
        // \- first
        // \- second
        // \- third
        //
        // 2. checkpoint before adding the third node
        // 
        // 3. the mount only includes the '/first' path, so only the 
        // 'second' and 'third' nodes should be available
        
        MemoryNodeStore seed = new MemoryNodeStore();
        NodeBuilder root = seed.getRoot().builder();
        root.child("first");
        root.child("second");
        seed.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        String checkpoint1 = seed.checkpoint(TimeUnit.MINUTES.toMillis(10));
        
        root = seed.getRoot().builder();
        root.child("third");
        seed.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        MemoryNodeStore target = new MemoryNodeStore();
        MountInfoProvider mip = Mounts.newBuilder().mount("seed", "/first").build();
        
        // perform migration
        InitialContentMigrator icm = new InitialContentMigrator(target, seed, mip.getMountByName("seed"));
        icm.migrate();
        
        NodeState targetRoot = target.getRoot();
        
        // verify that the 'second' and 'third' nodes are visible in the migrated store
        assertFalse("Node /first should not have been migrated", targetRoot.hasChildNode("first"));
        assertTrue("Node /second should have been migrated", targetRoot.hasChildNode("second"));
        assertTrue("Node /third should have been migrated", targetRoot.hasChildNode("third"));
        
        // verify that the 'second' node is visible in the migrated store when retrieving the checkpoint
        NodeState checkpointTargetRoot = target.retrieve(checkpoint1);
        assertFalse("Node /first should not have been migrated", checkpointTargetRoot.hasChildNode("first"));
        assertTrue("Node /second should have been migrated", checkpointTargetRoot.hasChildNode("second"));
        assertFalse("Node /third should not be visible from the migrated checkpoint", checkpointTargetRoot.hasChildNode("third"));

    }
    
}
