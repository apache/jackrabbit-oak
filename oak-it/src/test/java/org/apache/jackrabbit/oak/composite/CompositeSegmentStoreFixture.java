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

import java.io.IOException;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class CompositeSegmentStoreFixture extends NodeStoreFixture {

    private static final String MOUNT_PATH = "/tmp";

    @Override
    public NodeStore createNodeStore() {
        try {
            MountInfoProvider mip = Mounts.newBuilder()
                    .readOnlyMount("temp", MOUNT_PATH)
                    .build();

            NodeStore globalStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
            NodeStore tempMount = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();

            return new CompositeNodeStore.Builder(mip, globalStore).addMount("temp", tempMount).build();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " with a mount under " + MOUNT_PATH;
    }

}