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
package org.apache.jackrabbit.oak.composite.checks;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.mount.Mounts.defaultMountInfoProvider;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class NodeStoreChecksServiceTest {

    private MemoryNodeStore globalStore;
    private MemoryNodeStore mountedStore;
    private MountInfoProvider mip;
    private Mount mount;

    @Before
    public void createFixture() throws CommitFailedException {
        globalStore = new MemoryNodeStore();
        mountedStore = new MemoryNodeStore();        

        NodeBuilder rootBuilder = mountedStore.getRoot().builder();
        rootBuilder.setChildNode("first").setChildNode("second").setChildNode("third");
        rootBuilder.setChildNode("not-covered");
        mountedStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        mip = Mounts.newBuilder()
                .readOnlyMount("first", "/first")
                .build();
        
        mount = mip.getMountByName("first");
    }
    
    @Test
    public void noCheckers() throws CommitFailedException {

        NodeStoreChecksService checks = new NodeStoreChecksService();
        
        checks.check(globalStore, new MountedNodeStore(mount, mountedStore));
    }
    
    @Test(expected = IllegalRepositoryStateException.class)
    public void failOnNodeCoveredByMount() {

        NodeStoreChecksService checks = new NodeStoreChecksService(defaultMountInfoProvider(), Arrays.asList(new FailOnTreeNameChecker("third")));
        
        checks.check(globalStore, new MountedNodeStore(mount, mountedStore));
    }

    @Test
    public void doNotFailOnNodeNotCoveredByMount() {
        
        NodeStoreChecksService checks = new NodeStoreChecksService(defaultMountInfoProvider(), Arrays.asList(new FailOnTreeNameChecker("not-covered")));
        
        checks.check(globalStore, new MountedNodeStore(mount, mountedStore));
    }
    
    static class FailOnTreeNameChecker implements MountedNodeStoreChecker<Void> {
        
        private final String name;

        private FailOnTreeNameChecker(String name) {
            this.name = checkNotNull(name, "name shold not be null");
        }

        @Override
        public Void createContext(NodeStore globalStore, MountInfoProvider mip) {
            return null;
        }

        @Override
        public boolean check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Void context) {
            if ( name.equals(tree.getName()))
                errorHolder.report(mountedStore, tree.getPath(), "test failure");
            
            return true;
        }
        
    }
}
