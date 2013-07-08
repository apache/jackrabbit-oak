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

package org.apache.jackrabbit.oak.plugins.observation;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.RecursingNodeStateDiff;

/**
 * Base class for {@code NodeStateDiff} implementations that can be secured.
 * That is, its call back methods are only called when its receiver has sufficient
 * rights to access the respective items.
 * <p>
 * Implementors must implement the {@link #create(SecurableNodeStateDiff, String, NodeState, NodeState)}
 * factory method for creating {@code SecurableNodeStateDiff} instances for child nodes.
 * Further implementors should override {@link #canRead(PropertyState, PropertyState)} and
 * {@link #canRead(String, NodeState, NodeState)} and determine whether the passed states are
 * accessible and the respective callbacks should thus be invoked. Finally implementors should override,
 * {@link #secureBefore(String, NodeState)}, and {@link #secureAfter(String, NodeState)}} wrapping the
 * passed node state into a node state that restricts access to accessible child nodes and properties.
 */
public abstract class SecurableNodeStateDiff implements NodeStateDiff {

    /**
     * Parent diff
     */
    private final SecurableNodeStateDiff parent;

    /**
     * Unsecured diff this secured diff delegates to after it has determined
     * that the items pertaining to a call back are accessible.
     */
    private RecursingNodeStateDiff diff;

    /**
     * Deferred {@link #childNodeChanged(String, NodeState, NodeState)} calls.
     * Such calls are deferred until an accessible change in the respective sub tree
     * is detected, as otherwise we might leak information restricted by access control
     * to the call back.
     */
    private Deferred deferred = Deferred.EMPTY;

    private SecurableNodeStateDiff(SecurableNodeStateDiff parent, RecursingNodeStateDiff diff) {
        this.parent = parent;
        this.diff = diff;
    }

    /**
     * Create a new child instance
     * @param parent  parent of this instance
     */
    protected SecurableNodeStateDiff(SecurableNodeStateDiff parent) {
        this(parent, RecursingNodeStateDiff.EMPTY);
    }

    /**
     * Create a new instance wrapping a unsecured diff.
     * @param diff  unsecured diff
     */
    protected SecurableNodeStateDiff(RecursingNodeStateDiff diff) {
        this(null, diff);
    }

    /**
     * Factory method for creating {@code SecurableNodeStateDiff} instances for child nodes.
     * @param parent  parent diff
     * @param name    name of the child node
     * @param before  before state of the child node
     * @param after   after state of the child node
     * @return  {@code SecurableNodeStateDiff} for the child node {@code name}.
     */
    @CheckForNull
    protected abstract SecurableNodeStateDiff create(SecurableNodeStateDiff parent,
            String name, NodeState before, NodeState after);

    /**
     * Determine whether a property is accessible
     * @param before  before state of the property
     * @param after   after state of the property
     * @return  {@code true} if accessible, {@code false} otherwise.
     */
    protected boolean canRead(PropertyState before, PropertyState after) {
        return true;
    }

    /**
     * Determine whether a node is accessible
     * @param before  before state of the node
     * @param after   after state of the node
     * @return  {@code true} if accessible, {@code false} otherwise.
     */
    protected boolean canRead(String name, NodeState before, NodeState after) {
        return true;
    }

    /**
     * Secure the before state of a child node such that it only provides
     * accessible child nodes and properties.
     * @param name       name of the child node
     * @param nodeState  before state of the child node
     * @return  secured before state
     */
    @Nonnull
    protected NodeState secureBefore(String name, NodeState nodeState) {
        return nodeState;
    }

    /**
     * Secure the after state of a child node such that it only provides
     * accessible child nodes and properties.
     * @param name       name of the child node
     * @param nodeState  after state of the child node
     * @return  secured after state
     */
    @Nonnull
    protected NodeState secureAfter(String name, NodeState nodeState) {
        return nodeState;
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        if (canRead(null, after)) {
            return applyDeferred() && diff.propertyAdded(after);
        }
        else {
            return true;
        }
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        if (canRead(before, after)) {
            return applyDeferred() && diff.propertyChanged(before, after);
        }
        else {
            return true;
        }
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        if (canRead(before, null)) {
            return applyDeferred() && diff.propertyDeleted(before);
        }
        else {
            return true;
        }
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        if (canRead(name, null, after)) {
            return applyDeferred() && diff.childNodeAdded(name, secureAfter(name, after));
        } else {
            return true;
        }
    }

    @Override
    public boolean childNodeChanged(final String name, final NodeState before, final NodeState after) {
        final SecurableNodeStateDiff childDiff = create(this, name, before, after);
        if (childDiff == null) {
            // Continue with siblings but don't decent into this subtree
            return true;
        }

        // Defer call back until accessible changes in the subtree are found
        deferred = new Deferred() {
            @Override
            boolean call() {
                if (applyDeferred() && diff.childNodeChanged(name, secureBefore(name, before), secureAfter(name, after))) {
                    childDiff.diff = diff.createChildDiff(name, secureBefore(name, before), secureAfter(name, after));
                    return true;
                } else {
                    return false;
                }
            }
        };
        return after.compareAgainstBaseState(before, childDiff);
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        if (canRead(name, before, null)) {
            return applyDeferred() && diff.childNodeDeleted(name, secureBefore(name, before));
        } else {
            return true;
        }
    }

    private boolean applyDeferred() {
        return parent == null || parent.deferred.apply();
    }

    //------------------------------------------------------------< Deferred >---

    /**
     * A deferred method call implementing call by need semantics.
     */
    private abstract static class Deferred {
        public static final Deferred EMPTY = new Deferred() {
            @Override
            boolean call() {
                return true;
            }
        };

        private Boolean result;

        boolean apply() {
            if (result == null) {
                result = call();
            }
            return result;
        }
        abstract boolean call();
    }
}
