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

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * Helper class for comparing the equality of node states based on the
 * content diff mechanism.
 */
public class EqualsDiff implements NodeStateDiff {

    /**
     * Diffs the given node states and returns {@code true} if there are no
     * differences.
     *
     * @param before before state
     * @param after after state
     * @return {@code true} if the states are equal, {@code false} otherwise
     */
    public static boolean equals(NodeState before, NodeState after) {
        return before.exists() == after.exists()
                && after.compareAgainstBaseState(before, new EqualsDiff());
    }

    /**
     * Diffs the given node states and returns {@code true} if there are
     * differences within the properties or direct child nodes.
     *
     * @param before before state
     * @param after after state
     * @return {@code true} if there are modifications, {@code false} otherwise
     */
    public static boolean modified(NodeState before, NodeState after) {
        return !after.compareAgainstBaseState(before, new EqualsDiff() {
            @Override
            public boolean childNodeChanged(
                    String name, NodeState before, NodeState after) {
                return true;
            }
        });
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        return false;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        return false;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        return false;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        return false;
    }

    @Override
    public boolean childNodeChanged(
            String name, NodeState before, NodeState after) {
        return after.compareAgainstBaseState(before, this);
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        return false;
    }

}
