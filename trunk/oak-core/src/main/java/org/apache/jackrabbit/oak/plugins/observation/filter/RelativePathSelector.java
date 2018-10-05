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
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A selector for selecting a node at a relative path from the node selected by
 * an initial selector.
 * <p>
 * <em>Note:</em> selecting the parent of the root node will return a non existing
 * {@code NodeState} instance.
 */
public class RelativePathSelector implements Selector {
    private final Iterable<String> path;
    private final Selector selector;

    /**
     * @param path      path to select from
     * @param selector  selector to base {@code path} upon
     */
    public RelativePathSelector(@NotNull String path, @NotNull Selector selector) {
        this.path = elements(checkNotNull(path));
        this.selector = checkNotNull(selector);
    }

    @NotNull
    @Override
    public NodeState select(@NotNull UniversalFilter filter,
            @Nullable PropertyState before, @Nullable PropertyState after) {
        return select(selector.select(filter, before, after));
    }

    @NotNull
    @Override
    public NodeState select(@NotNull UniversalFilter filter,
            @NotNull String name, @NotNull NodeState before, @NotNull NodeState after) {
        return select(selector.select(filter, name, before, after));
    }

    //------------------------------------------------------------< internal >---

    private NodeState select(NodeState node) {
        for (String name : path) {
            node = node.getChildNode(name);
        }
        return node;
    }

}
