/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.commit;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This abstract class provides node-type extensible commit hooks. One can extend this class to create
 * {@link CommitHook}s for specific node types or mixin types. For more details check this class' constructor.
 */
public abstract class NodeTypeCommitHook implements CommitHook {

    protected String nodeType;

    /**
     * Creates a {@link NodeTypeCommitHook} for a specified node type or mixin type.
     * 
     * @param nodeType
     *            the type of the node / mixin
     */
    protected NodeTypeCommitHook(String nodeType) {
        this.nodeType = nodeType;
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder node = after.builder();
        after.compareAgainstBaseState(before, new ModifiedNodeStateDiff(nodeType, node));
        return node.getNodeState();
    }

    private class ModifiedNodeStateDiff extends DefaultNodeStateDiff {

        private final String nodeType;
        private final NodeBuilder nodeBuilder;

        public ModifiedNodeStateDiff(String nodeType, NodeBuilder nodeBuilder) {
            this.nodeType = nodeType;
            this.nodeBuilder = nodeBuilder;
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            boolean matchesNodeType = after.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue(STRING).equals(nodeType);
            boolean matchesMixin = false;
            PropertyState mixTypes = after.getProperty(JcrConstants.JCR_MIXINTYPES);
            if (mixTypes != null) {
                Iterable<String> mixins = mixTypes.getValue(STRINGS);
                for (String m : mixins) {
                    if (m.equals(nodeType)) {
                        matchesMixin = true;
                        break;
                    }
                }
            }
            if (matchesNodeType || matchesMixin) {
                processChangedNode(before, after, nodeBuilder.child(name));
            }
        }

    }

    /**
     * Will be called on all sub-classes of the {@link NodeTypeCommitHook} class when a change is detected on a node
     * with the specified primary type or mixin type.
     * 
     * @param before
     *            the state of node before the change
     * @param after
     *            the state of the node after the change
     * @param nodeBuilder
     *            a node builder to be used in case one wants to create new states
     */
    protected abstract void processChangedNode(NodeState before, NodeState after, NodeBuilder nodeBuilder);
}
