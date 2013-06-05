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

package org.apache.jackrabbit.oak.spi.state;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * {@code NodeStateDiff} wrapper that passes only changes to non-hidden nodes and properties
 * (i.e. ones whose names don't start with a colon) to the given delegate diff.
 *
 * @since Oak 0.9
 */
public class VisibleDiff implements NodeStateDiff {
    private final NodeStateDiff diff;

    @Nonnull
    public static NodeStateDiff wrap(@Nonnull NodeStateDiff diff) {
        return new VisibleDiff(checkNotNull(diff));
    }

    public VisibleDiff(NodeStateDiff diff) {
        this.diff = checkNotNull(diff);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        if (!isHidden(after.getName())) {
            return diff.propertyAdded(after);
        } else {
            return true;
        }
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        if (!isHidden(after.getName())) {
            return diff.propertyChanged(before, after);
        } else {
            return true;
        }
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        if (!isHidden(before.getName())) {
            return diff.propertyDeleted(before);
        } else {
            return true;
        }
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        if (!isHidden(name)) {
            return diff.childNodeAdded(name, after);
        } else {
            return true;
        }
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        if (!isHidden(name)) {
            return diff.childNodeChanged(name, before, after);
        } else {
            return true;
        }
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        if (!isHidden(name)) {
            return diff.childNodeDeleted(name, before);
        } else {
            return true;
        }
    }
}
