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
package org.apache.jackrabbit.oak.composite;

import java.util.Collections;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.composite.checks.NodeStoreChecksService;
import org.apache.jackrabbit.oak.composite.checks.NodeTypeMountedNodeStoreChecker;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

public class CompositeNodeStoreBuilderTest {

    @Test(expected = IllegalArgumentException.class)
    public void builderRejectsTooManyReadWriteStores_oneExtra() {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("temp", "/tmp")
                .build();

        new CompositeNodeStore.Builder(mip, new MemoryNodeStore())
            .addMount("temp", new MemoryNodeStore())
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void builderRejectsTooManyReadWriteStores_mixed() {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("temp", "/tmp")
                .readOnlyMount("readOnly", "/readOnly")
                .build();

        new CompositeNodeStore.Builder(mip, new MemoryNodeStore())
            .addMount("temp", new MemoryNodeStore())
            .addMount("readOnly", new MemoryNodeStore())
            .build();
    }

    @Test
    public void builderAcceptsMultipleReadOnlyStores() {
        MountInfoProvider mip = Mounts.newBuilder()
                .readOnlyMount("readOnly", "/readOnly")
                .readOnlyMount("readOnly2", "/readOnly2")
                .build();

        new CompositeNodeStore.Builder(mip, new MemoryNodeStore())
            .addMount("readOnly", new MemoryNodeStore())
            .addMount("readOnly2", new MemoryNodeStore())
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void mismatchBetweenMountsAndStoresIsRejected() {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("temp", "/tmp")
                .build();

        new CompositeNodeStore.Builder(mip, new MemoryNodeStore())
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void mismatchBetweenMountNameAndStoreName() {
        MountInfoProvider mip = Mounts.newBuilder()
                .mount("temp", "/tmp")
                .build();

        new CompositeNodeStore.Builder(mip, new MemoryNodeStore())
            .addMount("not-temp", new MemoryNodeStore())
            .build();
    }
    
    @Test(expected = IllegalRepositoryStateException.class)
    public void versionableNode() throws CommitFailedException {

        MemoryNodeStore root = new MemoryNodeStore();
        MemoryNodeStore mount = new MemoryNodeStore();
        
        // create a child node that is versionable
        // note that we won't cover all checks here, we are only interested in seeing that at least one check is triggered
        NodeBuilder rootBuilder = mount.getRoot().builder();
        NodeBuilder childNode = rootBuilder.setChildNode("readOnly").setChildNode("second").setChildNode("third");
        childNode.setProperty(JcrConstants.JCR_ISCHECKEDOUT, false);
        childNode.setProperty(PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES , Collections.singletonList(JcrConstants.MIX_VERSIONABLE), Type.NAMES));
        mount.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        MountInfoProvider mip = Mounts.newBuilder()
                .readOnlyMount("readOnly", "/readOnly")
                .build();

        new CompositeNodeStore.Builder(mip, root)
            .addMount("readOnly", mount)
            .with(new NodeStoreChecksService(mip, Collections.singletonList(new NodeTypeMountedNodeStoreChecker(JcrConstants.MIX_VERSIONABLE, "test error"))))
            .build();        
        
    }
}