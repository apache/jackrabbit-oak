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

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

/**
 * A {@link NodeStateDiff} implementation that counts the differences between
 * two node states, including their sub tree.
 */
public class CountingDiff implements NodeStateDiff {

    private int changes = 0;

    public static int countChanges(NodeState before, NodeState after) {
        CountingDiff counter = new CountingDiff();
        after.compareAgainstBaseState(before, counter);
        return counter.getNumChanges();
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        inc();
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        inc();
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        inc();
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        inc();
        return after.compareAgainstBaseState(EMPTY_NODE, this);
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        inc();
        return after.compareAgainstBaseState(before, this);
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        inc();
        return MISSING_NODE.compareAgainstBaseState(before, this);
    }

    public int getNumChanges() {
        return changes;
    }

    private void inc() {
        changes++;
    }
}
