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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.LazyValue;

/**
 * {@code EventTypeFilter} filters based on the path of the <em>associated parent node</em> as
 * defined by {@link javax.jcr.observation.ObservationManager#addEventListener(
        javax.jcr.observation.EventListener, int, String, boolean, String[], String[], boolean)
        ObservationManager.addEventListener()}.
 */
public class PathFilter implements Filter {
    private final ImmutableTree beforeTree;
    private final ImmutableTree afterTree;
    private final String path;
    private final boolean deep;

    private final LazyValue<Boolean> include = new LazyValue<Boolean>() {
        @Override
        protected Boolean createValue() {
            String associatedParentPath = afterTree.exists()
                    ? afterTree.getPath()
                    : beforeTree.getPath();

            boolean equalPaths = path.equals(associatedParentPath);
            if (!deep) {
                return equalPaths;
            } else {
                return equalPaths || isAncestor(path, associatedParentPath);
            }
        }
    };

    /**
     * Create a new {@code Filter} instance that includes an event when the path of the
     * associated parent node matches the one of this filter or - when the {@code deep}
     * flag is set - is a descendant of the path of this filter.
     *
     * @param beforeTree  associated parent before state
     * @param afterTree   associated parent after state
     * @param path        path to match
     */
    public PathFilter(@Nonnull ImmutableTree beforeTree, @Nonnull ImmutableTree afterTree,
            @Nonnull String path, boolean deep) {
        this.beforeTree = checkNotNull(beforeTree);
        this.afterTree = checkNotNull(afterTree);
        this.path = checkNotNull(path);
        this.deep = deep;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return include.get();
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return include.get();
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return include.get();
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return include.get();
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return include.get();
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return include.get();
    }

    @Override
    public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
        return include.get();
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        String associatedParentPath = afterTree.exists()
            ? PathUtils.concat(afterTree.getPath(), name)
            : PathUtils.concat(beforeTree.getPath(), name);

        if (isAncestor(associatedParentPath, path) ||
                path.equals(associatedParentPath) ||
                deep && isAncestor(path, associatedParentPath)) {
            return new PathFilter(beforeTree, afterTree.getChild(name), path, deep);
        } else {
            return null;
        }
    }

}
