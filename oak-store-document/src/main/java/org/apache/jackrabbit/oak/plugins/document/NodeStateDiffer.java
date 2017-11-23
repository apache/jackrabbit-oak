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

package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public interface NodeStateDiffer {
    NodeStateDiffer DEFAULT_DIFFER = new NodeStateDiffer() {
        @Override
        public boolean compare(@Nonnull AbstractDocumentNodeState node,
                               @Nonnull AbstractDocumentNodeState base, @Nonnull NodeStateDiff diff) {
            return node.compareAgainstBaseState(base, diff);
        }
    };


    /**
     * Compares the given {@code node} against the {@code base} state and
     * reports the differences to the {@link NodeStateDiff}.
     *
     * @param node the node to compare.
     * @param base the base node to compare against.
     * @param diff handler of node state differences
     * @return {@code true} if the full diff was performed, or
     *         {@code false} if it was aborted as requested by the handler
     *         (see the {@link NodeStateDiff} contract for more details)
     */
    boolean compare(@Nonnull final AbstractDocumentNodeState node,
                    @Nonnull final AbstractDocumentNodeState base,
                    @Nonnull NodeStateDiff diff);
}
