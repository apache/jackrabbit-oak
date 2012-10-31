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
package org.apache.jackrabbit.oak.plugins.nodetype;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * NodeTypeManager implementation based on a given {@code NodeState} in order
 * to be used for the various node type related validators.
 */
class ValidatingNodeTypeManager extends ReadWriteNodeTypeManager {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ValidatingNodeTypeManager.class);

    private final Tree types;

    ValidatingNodeTypeManager(NodeState nodeState) {
        this.types = getTypes(nodeState);
    }

    @Override
    protected Tree getTypes() {
        return types;
    }

    private Tree getTypes(NodeState after) {
        Tree tree = new ReadOnlyTree(after);
        for (String name : PathUtils.elements(NODE_TYPES_PATH)) {
            if (tree == null) {
                break;
            } else {
                tree = tree.getChild(name);
            }
        }
        return tree;
    }
}