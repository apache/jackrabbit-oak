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
package org.apache.jackrabbit.oak.spi.commit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Extension point for validating and modifying content changes. Available
 * commit hooks are called in sequence to process incoming content changes
 * before they get persisted and shared with other clients.
 * <p>
 * A commit hook can throw a {@link CommitFailedException} for a particular
 * change to prevent it from being persisted, or it can modify the changes
 * for example to update an in-content index or to add auto-generated content.
 * <p>
 * Note that instead of implementing this interface directly, most commit
 * editors and validators are better expressed as implementations of the
 * more specific extension interfaces defined in this package.
 *
 * @see <a href="http://jackrabbit.apache.org/oak/docs/nodestate.html#The_commit_hook_mechanism"
 *         >The commit hook mechanism</a>
 */
public interface CommitHook {

    /**
     * Validates and/or modifies the given content change before it gets
     * persisted.
     *
     * @param before content tree before the commit
     * @param after content tree prepared for the commit
     * @param info metadata associated with this commit
     * @return content tree to be committed
     * @throws CommitFailedException if the commit should be rejected
     */
    @Nonnull
    NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
        throws CommitFailedException;

}
