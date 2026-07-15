/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.util;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of a {@code NodeStateDiff} that reports the inverse operation
 * to the wrapped {@code NodeStateDiff}.
 */
public class ReverseNodeStateDiff implements NodeStateDiff {

    private final NodeStateDiff diff;

    public ReverseNodeStateDiff(NodeStateDiff diff) {
        this.diff = requireNonNull(diff);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        return diff.propertyDeleted(after);
    }

    @Override
    public boolean propertyChanged(PropertyState before,
                                   PropertyState after) {
        return diff.propertyChanged(after, before);
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        return diff.propertyAdded(before);
    }

    @Override
    public boolean childNodeAdded(String name,
                                  NodeState after) {
        return diff.childNodeDeleted(name, after);
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        return diff.childNodeChanged(name, after, before);
    }

    @Override
    public boolean childNodeDeleted(String name,
                                    NodeState before) {
        return diff.childNodeAdded(name, before);
    }
}
