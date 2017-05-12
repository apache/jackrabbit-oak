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

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
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
}