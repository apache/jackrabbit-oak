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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

public abstract class SecurableNodeStateDiff implements NodeStateDiff {
    private final SecurableNodeStateDiff parent;

    private RecursingNodeStateDiff diff;
    private Deferred deferred = Deferred.EMPTY;

    private SecurableNodeStateDiff(SecurableNodeStateDiff parent, RecursingNodeStateDiff diff) {
        this.diff = diff;
        this.parent = parent;
    }

    protected SecurableNodeStateDiff(SecurableNodeStateDiff parent) {
        this(parent, RecursingNodeStateDiff.EMPTY);
    }

    protected SecurableNodeStateDiff(RecursingNodeStateDiff diff) {
        this(null, diff);
    }

    protected abstract SecurableNodeStateDiff create(SecurableNodeStateDiff parent,
            String name, NodeState before, NodeState after);

    protected boolean canRead(PropertyState before, PropertyState after) {
        return true;
    }

    protected boolean canRead(String name, NodeState before, NodeState after) {
        return true;
    }

    protected NodeState secureBefore(String name, NodeState nodeState) {
        return nodeState;
    }

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
