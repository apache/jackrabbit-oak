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
package org.apache.jackrabbit.oak.plugins.index;

/**
 * Implemented by index editor providers that support per-target catch-up indexing.
 *
 * <p>When a new target is added to {@code storeTargets} on an existing index,
 * the catch-up mechanism runs an {@code EditorDiff} from the last known checkpoint
 * (or {@link #CATCH_UP_FROM_START} for a full traversal) to the current lane
 * checkpoint, without affecting any other target.</p>
 *
 * <p>The tracking state is kept under a {@value #CATCH_UP_TRACKING_NODE} child
 * node on the index definition. Each property on that node names a target type
 * (as it appears in {@code storeTargets}) and holds the checkpoint up to which
 * that target has been indexed. A missing property means the target is in sync
 * with the lane.</p>
 */
public interface CatchUpCapable {

    /**
     * Sentinel value stored on the tracking node to request a full traversal
     * (equivalent to {@code reindex=true} but scoped to a single target).
     */
    String CATCH_UP_FROM_START = "INITIAL";

    /**
     * Name of the child node under each index definition that holds
     * per-target catch-up state.
     */
    String CATCH_UP_TRACKING_NODE = "tracking";

    // Marker interface - providers that implement this support catch-up indexing.
    // Catch-up uses the same getIndexEditor() method as normal indexing, just with
    // a different targetType and checkpoint management.
}
