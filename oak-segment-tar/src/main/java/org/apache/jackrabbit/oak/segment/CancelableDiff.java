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
package org.apache.jackrabbit.oak.segment;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A {@code NodeStateDiff} that cancels itself when a condition occurs. The
 * condition is represented by an externally provided instance of {@code
 * Supplier}. If the {@code Supplier} returns {@code true}, the diffing process
 * will be canceled at the first possible occasion.
 */
public class CancelableDiff implements NodeStateDiff {

    private final NodeStateDiff delegate;

    private final Supplier<Boolean> canceled;

    public CancelableDiff(NodeStateDiff delegate, Supplier<Boolean> canceled) {
        this.delegate = delegate;
        this.canceled = canceled;
    }

    @Override
    public final boolean propertyAdded(PropertyState after) {
        if (canceled.get()) {
            return false;
        }

        return delegate.propertyAdded(after);
    }

    @Override
    public final boolean propertyChanged(PropertyState before, PropertyState after) {
        if (canceled.get()) {
            return false;
        }

        return delegate.propertyChanged(before, after);
    }

    @Override
    public final boolean propertyDeleted(PropertyState before) {
        if (canceled.get()) {
            return false;
        }

        return delegate.propertyDeleted(before);
    }

    @Override
    public final boolean childNodeAdded(String name, NodeState after) {
        if (canceled.get()) {
            return false;
        }

        return delegate.childNodeAdded(name, after);
    }

    @Override
    public final boolean childNodeChanged(String name, NodeState before, NodeState after) {
        if (canceled.get()) {
            return false;
        }

        return delegate.childNodeChanged(name, before, after);
    }

    @Override
    public final boolean childNodeDeleted(String name, NodeState before) {
        if (canceled.get()) {
            return false;
        }

        return delegate.childNodeDeleted(name, before);
    }

}
