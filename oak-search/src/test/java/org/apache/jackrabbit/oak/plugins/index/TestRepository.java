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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class TestRepository {

    private AsyncIndexUpdate asyncIndexUpdate;

    public TestRepository(Oak oak) {
        this.oak = oak;
    }

    public enum NodeStoreType {
        MEMORY_NODE_STORE
    }

    public final int defaultAsyncIndexingTimeInSeconds = 5;

    private boolean isAsync;

    protected NodeStore nodeStore;
    protected Oak oak;

    public Oak getOak() {
        return oak;
    }

    public TestRepository with(boolean isAsync) {
        this.isAsync = isAsync;
        return this;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public TestRepository with(AsyncIndexUpdate asyncIndexUpdate) {
        this.asyncIndexUpdate = asyncIndexUpdate;
        return this;
    }
}
