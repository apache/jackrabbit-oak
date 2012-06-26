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
 * Extension point for validating and modifying content changes. Available
 * commit editors are called in sequence to process incoming content changes
 * before they get persisted and shared with other clients.
 * <p>
 * A commit editor can throw a {@link CommitFailedException} for a particular
 * change to prevent it from being persisted, or it can modify the changes
 * for example to update an in-content index or to add auto-generated content.
 * <p>
 * Note that instead of implementing this interface directly, most commit
 * editors and validators are better expressed as implementations of the
 * more specific extension interfaces defined in this package.
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

}
