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
package org.apache.jackrabbit.oak.spi.lifecycle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * <code>CompositeMicroKernelTracker</code> consists of multiple micro kernel
 * trackers and calls them all on {@link #available(MicroKernel)}.
 */
public class CompositeMicroKernelTracker implements MicroKernelTracker {

    private final List<MicroKernelTracker> trackers = new ArrayList<MicroKernelTracker>();

    public CompositeMicroKernelTracker(Collection<MicroKernelTracker> trackers) {
        this.trackers.addAll(trackers);
    }

    public CompositeMicroKernelTracker(MicroKernelTracker... trackers) {
        this.trackers.addAll(Arrays.asList(trackers));
    }

    @Override
    public void available(NodeStore store) {
        for (MicroKernelTracker tracker : trackers) {
            tracker.available(store);
        }
    }
}
