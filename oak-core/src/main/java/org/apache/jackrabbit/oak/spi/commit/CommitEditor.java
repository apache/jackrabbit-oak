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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import javax.annotation.Nonnull;

/**
 * Extension point for custom functionality to be performed before and
 * after content changes are committed. The repository core will call
 * all available commit hooks in sequence for all commits it makes or
 * sees. The control flows roughly like this:
 * <pre>
 * NodeStore store = ...;
 * NodeState before = ...;
 * NodeState after = ...;
 *
 * for (CommitHook hook : hooks) {
 *     after = hook.preCommit(store, before, after);
 * }
 *
 * after = branch.merge();
 *
 * for (CommitHook hook : hooks) {
 *     hook.afterCommit(store, before, after);
 * }
 * </pre>
 * <p>
 * Note that instead of implementing this interface directly, most commit
 * hooks are better expressed as implementations of the more specific
 * extension interfaces defined in this package.
 */
public interface CommitEditor {

    /**
     * Before-commit hook. The implementation can validate, record or
     * modify the staged commit. After all available before-commit hooks
     * have been processed and none of them has thrown an exception the
     * collected changes are committed to the underlying storage model.
     * <p>
     * Note that a before-commit hook can be executed multiple times for
     * the same change, for example when a change needs to be retried
     * after possible merge conflicts have been resolved. Use the
     * after-commit hook if you need to be notified only once for each
     * change.
     *
     * @param store the node store that contains the repository content
     * @param before content tree before the commit
     * @param after content tree prepared for the commit
     * @return content tree to be committed
     * @throws CommitFailedException if the commit should be rejected
     */
    @Nonnull
    NodeState beforeCommit(NodeStore store, NodeState before, NodeState after)
        throws CommitFailedException;

    /**
     * After-commit hook. The implementation can no longer modify the commit
     * or make it fail due to an exception, but may update caches, trigger
     * observation events or otherwise record the change.
     * <p>
     * After-commit hooks are called both for commits made locally against
     * the repository instance to which the hook is registered and for any
     * other changes committed by other repository instances in the same
     * cluster.
     * <p>
     * The after-commit hooks do not necessarily reflect each individual
     * commit made against the repository, but rather more generic updates
     * of the repository state that may include other, concurrently merged
     * changes. However, the repository does guarantee that a given state is
     * never returned by {@link NodeStore#getRoot()} before the respective
     * after-commit hooks have been called. Also, the after-commit hooks are
     * all linear in the sense that the {@code after} state of one hook
     * invocation is guaranteed to be the {@code before} state of the next
     * hook invocation. This sequence of changes only applies while a hook
     * is registered with a specific repository instance, and is thus for
     * example <em>not</me> guaranteed across repository restarts.
     * <p>
     * After-commit hooks are executed synchronously within the context of
     * a repository instance, so to prevent delaying access to latest changes
     * the after-commit hooks should avoid any potentially blocking
     * operations.
     *
     * @param store the node store that contains the repository content
     * @param before content tree before the commit
     * @param after content tree after the commit
     */
    void afterCommit(NodeStore store, NodeState before, NodeState after);

}
