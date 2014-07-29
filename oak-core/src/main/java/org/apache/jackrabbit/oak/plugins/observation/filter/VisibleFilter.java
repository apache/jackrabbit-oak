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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Event filter that hides all non-visible content.
 */
public class VisibleFilter implements EventFilter {
    public static VisibleFilter VISIBLE_FILTER = new VisibleFilter();

    private VisibleFilter() {}

    private static boolean isVisible(String name) {
        return !name.startsWith(":");
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return isVisible(after.getName());
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return isVisible(after.getName());
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return isVisible(before.getName());
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return isVisible(name);
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return isVisible(name);
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        for (String element : PathUtils.elements(sourcePath)) {
            if (!isVisible(element)) {
                return false;
            }
        }
        return isVisible(name);
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        return isVisible(destName) && isVisible(name);
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        if (isVisible(name)) {
            return this;
        } else {
            return null;
        }
    }

}
