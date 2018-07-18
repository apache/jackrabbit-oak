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
package org.apache.jackrabbit.oak.upgrade.nodestate;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.migration.AbstractDecoratedNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
public class NameFilteringNodeState extends AbstractDecoratedNodeState {

    private static final Logger LOG = LoggerFactory.getLogger(NameFilteringNodeState.class);

    private final NameFilteringNodeState parent;

    private final String name;

    public static NodeState wrapRoot(final NodeState delegate) {
        return new NameFilteringNodeState(delegate, null, null);
    }

    private NameFilteringNodeState(final NodeState delegate, NameFilteringNodeState parent, String name) {
        super(delegate, false);
        this.parent = parent;
        this.name = name;
    }

    @Override
    protected boolean hideChild(@NotNull final String name, @NotNull final NodeState delegateChild) {
        if (isNameTooLong(name)) {
            LOG.warn("Node name '{}' too long. Skipping child of {}", name, this);
            return true;
        }
        return super.hideChild(name, delegateChild);
    }

    @Override
    @NotNull
    protected NodeState decorateChild(@NotNull final String name, @NotNull final NodeState delegateChild) {
        return new NameFilteringNodeState(delegateChild, this, name);
    }

    @Override
    protected PropertyState decorateProperty(@NotNull final PropertyState delegatePropertyState) {
        return fixChildOrderPropertyState(this, delegatePropertyState);
    }

    /**
     * This method checks whether the name is no longer than the maximum node
     * name length supported by the DocumentNodeStore.
     *
     * @param name
     *            to check
     * @return true if the name is longer than {@link org.apache.jackrabbit.oak.plugins.document.util.Utils#NODE_NAME_LIMIT}
     */
    private boolean isNameTooLong(@NotNull String name) {
        // OAK-1589: maximum supported length of name for DocumentNodeStore
        // is 150 bytes. Skip the sub tree if the the name is too long
        if (name.length() <= Utils.NODE_NAME_LIMIT / 3) {
            return false;
        }
        if (name.getBytes(Charsets.UTF_8).length <= Utils.NODE_NAME_LIMIT) {
            return false;
        }
        String path = getPath();
        if (path.length() <= Utils.PATH_SHORT) {
            return false;
        }
        if (path.getBytes(Charsets.UTF_8).length < Utils.PATH_LONG) {
            return false;
        }
        return true;
    }

    private String getPath() {
        List<String> names = new ArrayList<>();
        NameFilteringNodeState ns = this;
        while (ns.parent != null) {
            names.add(ns.name);
            ns = ns.parent;
        }
        String[] reversed = new String[names.size()];

        int i = reversed.length - 1;
        for (String name : names) {
            reversed[i--] = name;
        }

        return PathUtils.concat(PathUtils.ROOT_PATH, reversed);
    }
}
