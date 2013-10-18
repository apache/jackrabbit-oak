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
package org.apache.jackrabbit.mk.test;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.kernel.NodeStoreKernel;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class SegmentMicroKernelFixture implements MicroKernelFixture {

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void setUpCluster(MicroKernel[] cluster) {
        NodeStore store = new SegmentNodeStore(new MemoryStore());
        MicroKernel mk = new NodeStoreKernel(store);
        for (int i = 0; i < cluster.length; i++) {
            cluster[i] = mk;
        }
    }

    @Override
    public void syncMicroKernelCluster(MicroKernel... nodes) {
    }

    @Override
    public void tearDownCluster(MicroKernel[] cluster) {
    }

}
