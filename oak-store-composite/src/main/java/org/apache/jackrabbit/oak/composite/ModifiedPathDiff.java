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
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import java.util.HashSet;
import java.util.Set;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

public class ModifiedPathDiff implements NodeStateDiff {

    private final Set<String> paths;

    private final String currentPath;

    public static Set<String> getModifiedPaths(NodeState before, NodeState after) {
        ModifiedPathDiff diff = new ModifiedPathDiff();
        after.compareAgainstBaseState(before, diff);
        return diff.getPaths();
    }

    private ModifiedPathDiff() {
        this.paths = new HashSet<>();
        this.currentPath = "/";
    }

    private ModifiedPathDiff(ModifiedPathDiff parent, String name) {
        this.paths = parent.paths;
        this.currentPath = concat(parent.currentPath, name);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        paths.add(currentPath);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        paths.add(currentPath);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        paths.add(currentPath);
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        paths.add(concat(currentPath, name));
        return true;
    }

    @Override
    public boolean childNodeChanged(String name, NodeState before, NodeState after) {
        return after.compareAgainstBaseState(before, new ModifiedPathDiff(this, name));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        paths.add(concat(currentPath, name));
        return true;
    }

    private Set<String> getPaths() {
        return paths;
    }
}
