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

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Content change editor. An editor receives information about changes
 * to the content tree and can reject the changes by throwing a
 * {@link CommitFailedException} or adjust them using the {@link NodeBuilder}
 * instance passed to the {@link EditorProvider} that returned this editor.
 * Note that the given builder can contain updates from multiple different
 * editors, so its state might not match exactly the state of the given
 * after state.
 *
 * @since Oak 0.7
 * @see <a href="http://jackrabbit.apache.org/oak/docs/nodestate.html#Commit_editors"
 *         >Commit editors</a>
 */
public interface Editor {

    /**
     * Called before the given before and after states are compared.
     * The implementation can use this method to initialize any internal
     * state needed for processing the results of the comparison. For example
     * an implementation could look up the effective node type of the after
     * state to know what constraints to apply to on the content changes.
     *
     * @param before before state, non-existent if this node was added
     * @param after after state, non-existent if this node was removed
     * @throws CommitFailedException if this commit should be rejected
     */
    void enter(NodeState before, NodeState after) throws CommitFailedException;

    /**
     * Called after the given before and after states are compared.
     * The implementation can use this method to post-process information
     * collected during the content diff. For example an implementation that
     * during the diff just recorded the fact that this node was modified
     * in some way could then use this method to trigger an index update
     * based on that modification flag.
     *
     * @param before before state, non-existent if this node was added
     * @param after after state, non-existent if this node was removed
     * @throws CommitFailedException if this commit should be rejected
     */
    void leave(NodeState before, NodeState after) throws CommitFailedException;

    /**
     * Processes an added property.
     *
     * @param after the added property
     * @throws CommitFailedException if processing failed
     */
    void propertyAdded(PropertyState after) throws CommitFailedException;

    /**
     * Processes a changed property.
     *
     * @param before the original property
     * @param after  the changed property
     * @throws CommitFailedException if processing failed
     */
    void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException;

    /**
     * Processes a removed property.
     *
     * @param before the removed property
     * @throws CommitFailedException if processing failed
     */
    void propertyDeleted(PropertyState before) throws CommitFailedException;

    /**
     * Processes an added child node.
     *
     * @param name name of the added node
     * @param after the added child node
     * @return an editor for processing the subtree below the added node,
     *         or {@code null} if the subtree does not need processing
     * @throws CommitFailedException if processing failed
     */
    @CheckForNull
    Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException;

    /**
     * Processes a changed child node. This method gets called for all child nodes
     * that may contain changes between the before and after states.
     *
     * @param name name of the changed node
     * @param before child node before the change
     * @param after child node after the change
     * @return an editor for processing the subtree below the added node,
     *         or {@code null} if the subtree does not need processing
     * @throws CommitFailedException if processing failed
     */
    @CheckForNull
    Editor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException;

    /**
     * Processes a deleted child node.
     *
     * @param name name of the deleted node
     * @param before the deleted child node
     * @return an editor for processing the subtree below the removed node,
     *         or {@code null} if the subtree does not need processing
     * @throws CommitFailedException if processing failed
     */
    @CheckForNull
    Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException;

}
