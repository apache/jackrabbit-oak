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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO document
 */
public class ConstantFilter implements EventFilter {
    public static final ConstantFilter INCLUDE_ALL = new ConstantFilter(true);
    public static final ConstantFilter EXCLUDE_ALL = new ConstantFilter(false);

    private final boolean include;

    public ConstantFilter(boolean include) {
        this.include = include;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return include;
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return include;
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return include;
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return include;
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return include;
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        return include;
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        return include;
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        return include ? this : null;
    }
}
