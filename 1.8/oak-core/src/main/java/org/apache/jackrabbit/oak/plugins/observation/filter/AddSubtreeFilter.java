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

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This {@code EventFilter} implementation excludes events for child nodes
 * of added nodes.
 */
public class AddSubtreeFilter implements EventFilter {
    private static final AddSubtreeFilter INCLUDE_ADD = new AddSubtreeFilter(true);
    private static final AddSubtreeFilter EXCLUDE_ADD = new AddSubtreeFilter(false);

    private final boolean include;

    public static EventFilter getInstance() {
        return INCLUDE_ADD;
    }

    private AddSubtreeFilter(boolean include) {
        this.include = include;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return true;
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return true;
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return true;
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return include;
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return true;
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        return true;
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        return true;
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        if (this == EXCLUDE_ADD) {
            return null;
        } else if (before == MISSING_NODE) {
            return EXCLUDE_ADD;
        } else {
            return INCLUDE_ADD;
        }
    }
}
