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
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO NodeTypeFilter...
 * TODO Clarify: filter applies to parent
 */
public class NodeTypeFilter implements Filter {
    private final ImmutableTree afterTree;
    private final ReadOnlyNodeTypeManager ntManager;
    private final String[] ntNames;

    public NodeTypeFilter(ImmutableTree afterTree, ReadOnlyNodeTypeManager ntManager, String[] ntNames) {
        this.afterTree = afterTree;
        this.ntManager = ntManager;
        this.ntNames = ntNames;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return includeByType();
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return includeByType();
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return includeByType();
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return includeByType();
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return true;
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return includeByType();
    }

    @Override
    public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
        return includeByType();
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        return new NodeTypeFilter(afterTree.getChild(name), ntManager, ntNames);
    }

    //------------------------------------------------------------< private >---

    /**
     * Checks whether to include an event based on the type of the associated
     * parent node and the node type filter.
     *
     * @return whether to include the event based on the type of the associated
     *         parent node.
     */
    private boolean includeByType() {
        if (ntNames == null) {
            return true;
        } else {
            for (String ntName : ntNames) {
                if (ntManager.isNodeType(afterTree, ntName)) {
                    return true;
                }
            }
            return false;
        }
    }

}
