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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.MoveValidator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@link Filter filter} and report changes between node states to the {@link Listener}.
 */
public class EventGenerator implements MoveValidator {
    private final Filter filter;
    private final Listener listener;

    /**
     * Filter for determining what changes to report the the {@link Listener}.
     */
    public interface Filter {

        /**
         * Include an added property
         * @param after  added property
         * @return  {@code true} if the property should be included
         */
        boolean includeAdd(PropertyState after);

        /**
         * Include a changed property
         * @param before  property before the change
         * @param after  property after the change
         * @return  {@code true} if the property should be included
         */
        boolean includeChange(PropertyState before, PropertyState after);

        /**
         * Include a deleted property
         * @param before  deleted property
         * @return  {@code true} if the property should be included
         */
        boolean includeDelete(PropertyState before);

        /**
         * Include an added node
         * @param name name of the node
         * @param after  added node
         * @return  {@code true} if the node should be included
         */
        boolean includeAdd(String name, NodeState after);

        /**
         * Include a changed node
         * @param name name of the node
         * @param before node before the change
         * @param after  node after the change
         * @return  {@code true} if the node should be included
         */
        boolean includeChange(String name, NodeState before, NodeState after);

        /**
         * Include a deleted node
         * @param name name of the node
         * @param before deleted node
         * @return  {@code true} if the node should be included
         */
        boolean includeDelete(String name, NodeState before);

        /**
         * Include a moved node
         * @param sourcePath  source path of the move operation
         * @param destPath  destination path of the move operation
         * @param moved the moved node
         * @return  {@code true} if the node should be included
         */
        boolean includeMove(String sourcePath, String destPath, NodeState moved);

        /**
         * Factory for creating a filter instance for the given child node
         * @param name  name of the child node
         * @param before  before state of the child node
         * @param after  after state of the child node
         * @return  filter instance for filtering the child node or {@code null} to
         *          exclude the sub tree rooted at this child node.
         */
        @CheckForNull
        Filter create(String name, NodeState before, NodeState after);
    }

    /**
     * Listener for listening to changes.
     */
    public interface Listener {

        /**
         * Notification for an added property
         * @param after  added property
         */
        void propertyAdded(PropertyState after);

        /**
         * Notification for a changed property
         * @param before  property before the change
         * @param after  property after the change
         */
        void propertyChanged(PropertyState before, PropertyState after);

        /**
         * Notification for a deleted property
         * @param before  deleted property
         */
        void propertyDeleted(PropertyState before);

        /**
         * Notification for an added node
         * @param name  name of the node
         * @param after  added node
         */
        void childNodeAdded(String name, NodeState after);

        /**
         * Notification for a changed node
         * @param name  name of the node
         * @param before  node before the change
         * @param after  node after the change
         */
        void childNodeChanged(String name, NodeState before, NodeState after);

        /**
         * Notification for a deleted node
         * @param name  name of the deleted node
         * @param before  deleted node
         */
        void childNodeDeleted(String name, NodeState before);

        /**
         * Notification for a moved node
         * @param sourcePath  source of the moved node
         * @param destPath  destination of the moved node
         * @param moved  moved node
         */
        void nodeMoved(String sourcePath, String destPath, NodeState moved);

        /**
         * Factory for creating a filter instance for the given child node
         * @param name name of the child node
         * @param before  before state of the child node
         * @param after  after state of the child node
         * @return  listener for the child node
         */
        @Nonnull
        Listener create(String name, NodeState before, NodeState after);
    }

    /**
     * Create a new instance of a {@code EventGenerator} reporting events to the
     * passed {@code listener} after filtering with the passed {@code filter}.
     * @param filter  filter for filtering changes
     * @param listener  listener for listening to the filtered changes
     */
    public EventGenerator(@Nonnull Filter filter, @Nonnull Listener listener) {
        this.filter = checkNotNull(filter);
        this.listener = checkNotNull(listener);
    }

    @Override
    public void move(String sourcePath, String destPath, NodeState moved) throws CommitFailedException {
        if (filter.includeMove(sourcePath, destPath, moved)) {
            listener.nodeMoved(sourcePath, destPath, moved);
        }
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (filter.includeAdd(after)) {
            listener.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (filter.includeChange(before, after)) {
            listener.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (filter.includeDelete(before)) {
            listener.propertyDeleted(before);
        }
    }

    @Override
    public MoveValidator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (filter.includeAdd(name, after)) {
            listener.childNodeAdded(name, after);
        }
        return createChildGenerator(name, MISSING_NODE, after);
    }

    @Override
    public MoveValidator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        if (filter.includeChange(name, before, after)) {
            listener.childNodeChanged(name, before, after);
        }
        return createChildGenerator(name, before, after);
    }

    @Override
    public MoveValidator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (filter.includeDelete(name, before)) {
            listener.childNodeDeleted(name, before);
        }
        return createChildGenerator(name, before, MISSING_NODE);
    }

    /**
     * Factory method for creating {@code EventGenerator} instances of child nodes.
     * @param name  name of the child node
     * @param before  before state of the child node
     * @param after  after state of the child node
     * @return {@code EventGenerator} for a child node
     */
    protected EventGenerator createChildGenerator(String name, NodeState before, NodeState after) {
        Filter childFilter = filter.create(name, before, after);
        if (childFilter != null) {
            return new EventGenerator(
                    childFilter,
                    listener.create(name, before, after));
        } else {
            return null;
        }
    }
}
