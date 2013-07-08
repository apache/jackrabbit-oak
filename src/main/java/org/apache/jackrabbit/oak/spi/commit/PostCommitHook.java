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

package org.apache.jackrabbit.oak.spi.commit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Extension point for observing content changes of an Oak
 * {@link org.apache.jackrabbit.oak.api.Root}. Content changes are
 * reported by passing the <em>before</em> and <em>after</em> state of the content
 * tree to the {@link #contentChanged(NodeState, NodeState)} callback
 * method.
 */
public interface PostCommitHook {
    PostCommitHook EMPTY = new PostCommitHook() {
        @Override
        public void contentChanged(@Nonnull NodeState before, @Nonnull NodeState after) { }
    };

    /**
     * Observes a content change on the associated {@link org.apache.jackrabbit.oak.api.Root}.
     * <p>
     * Post-commit hooks are executed synchronously within the context of
     * a repository instance, so to prevent delaying access to latest changes
     * the after-commit hooks should avoid any potentially blocking operations.
     *
     * @param before content tree before the commit
     * @param after content tree after the commit
     */
    void contentChanged(@Nonnull NodeState before, @Nonnull NodeState after);
}
