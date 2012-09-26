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

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Extension point for observing changes in an Oak repository. Content
 * changes are reported by passing the "before" and "after" state of the
 * content tree to the {@link #contentChanged(NodeState, NodeState)}
 * callback method.
 * <p>
 * Each observer is guaranteed to see a linear sequence of changes, i.e.
 * the "after" state of one method call is guaranteed to be the "before"
 * state of the following call. This sequence of changes only applies while
 * a hook is registered with a specific repository instance, and is thus for
 * example <em>not</me> guaranteed across repository restarts.
 * <p>
 * Note also that two observers may not necessarily see the same sequence of
 * content changes, and each commit does not necessarily trigger a separate
 * observer callback. It is also possible for an observer to be notified
 * when no actual changes have been committed.
 * <p>
 * A specific implementation or deployment may offer more guarantees about
 * when and how observers are notified of content changes. See the relevant
 * documentation for more details about such cases.
 *
 * @since Oak 0.3
 */
public interface Observer {

    /**
     * Observes a content change. The implementation might for example
     * use this information to update caches, trigger JCR-level observation
     * events or otherwise record the change.
     * <p>
     * Content changes are observed for both commits made locally against
     * the repository instance to which the hook is registered and any
     * other changes committed by other repository instances in the same
     * cluster.
     * <p>
     * After-commit hooks are executed synchronously within the context of
     * a repository instance, so to prevent delaying access to latest changes
     * the after-commit hooks should avoid any potentially blocking
     * operations.
     *
     * @param before content tree before the changes
     * @param after content tree after the changes
     */
    void contentChanged(NodeState before, NodeState after);

}
