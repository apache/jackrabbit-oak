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

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public abstract class Compactor {
    public final @Nullable CompactedNodeState compactDown(
            @NotNull NodeState state,
            @NotNull Canceller hardCanceller,
            @NotNull Canceller softCanceller
    ) throws IOException {
        return compactDown(EMPTY_NODE, state, hardCanceller, softCanceller);
    }

    /**
     * compact the differences between {@code after} and {@code before} on top of {@code after}.
     * @param before        the node state to diff against from {@code after}
     * @param after         the node state diffed against {@code before}
     * @param hardCanceller the trigger for hard cancellation, will abandon compaction if cancelled
     * @param softCanceller the trigger for soft cancellation, will return partially compacted state if cancelled
     * @return              the compacted node state or {@code null} if hard-cancelled
     * @throws IOException  will throw exception if any errors occur during compaction
     */
    public abstract @Nullable CompactedNodeState compactDown(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Canceller hardCanceller,
            @NotNull Canceller softCanceller
    ) throws IOException;

    public final @Nullable CompactedNodeState compactUp(
            @NotNull NodeState state,
            @NotNull Canceller canceller
    ) throws IOException {
        return compactUp(EMPTY_NODE, state, canceller);
    }

    /**
     * compact the differences between {@code after} and {@code before} on top of {@code before}.
     */
    public final @Nullable CompactedNodeState compactUp(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull Canceller canceller
    ) throws IOException {
        return compact(before, after, before, canceller);
    }

    /**
     * compact the differences between {@code after} and {@code before} on top of {@code onto}.
     * @param before        the node state to diff against from {@code after}
     * @param after         the node state diffed against {@code before}
     * @param onto          the node state to compact to apply the diff to
     * @param canceller     the trigger for hard cancellation, will abandon compaction if cancelled
     * @return              the compacted node state or {@code null} if hard-cancelled
     * @throws IOException  will throw exception if any errors occur during compaction
     */
    public abstract @Nullable CompactedNodeState compact(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            @NotNull Canceller canceller
    ) throws IOException;
}
