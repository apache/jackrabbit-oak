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
package org.apache.jackrabbit.oak.core;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

import static org.apache.jackrabbit.oak.core.TreeImpl.OAK_CHILD_ORDER;

/**
 * Maintains the {@link TreeImpl#OAK_CHILD_ORDER} property for nodes that have
 * orderable children. The class TreeImpl maintains the property as well for
 * transient operations. This class makes sure the child order property is kept
 * up-to-date when operations are performed on the {@link Root}. This includes
 * {@link Root#copy(String, String)} and {@link Root#move(String, String)}.
 */
public class OrderedChildrenEditor implements CommitHook {

    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        NodeBuilder builder = after.builder();

        after.compareAgainstBaseState(before,
                new OrderedChildrenDiff(builder));

        return builder.getNodeState();
    }

    private static class OrderedChildrenDiff extends DefaultNodeStateDiff {

        private final NodeBuilder builder;

        OrderedChildrenDiff(NodeBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (!NodeStateUtils.isHidden(name)) {
                updateChildOrder();
                NodeBuilder childBuilder = builder.child(name);
                OrderedChildrenDiff diff = new OrderedChildrenDiff(childBuilder);
                for (ChildNodeEntry entry : after.getChildNodeEntries()) {
                    diff.childNodeAdded(entry.getName(), entry.getNodeState());
                }
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            updateChildOrder();
        }

        @Override
        public void childNodeChanged(String name,
                                     NodeState before,
                                     NodeState after) {
            if (!NodeStateUtils.isHidden(name)) {
                NodeBuilder childBuilder = builder.child(name);
                OrderedChildrenDiff diff = new OrderedChildrenDiff(childBuilder);
                after.compareAgainstBaseState(before, diff);
            }
        }

        private void updateChildOrder() {
            PropertyState childOrder = builder.getProperty(OAK_CHILD_ORDER);
            if (childOrder != null) {
                PropertyBuilder<String> children = MemoryPropertyBuilder.create(Type.STRING, childOrder);
                // Remove removed
                for (String name : children.getValues()) {
                    if (NodeStateUtils.isHidden(name) || !builder.hasChildNode(name)) {
                        children.removeValue(name);
                    }
                }
                // Add added
                for (String name : builder.getChildNodeNames()) {
                    if (!NodeStateUtils.isHidden(name) && !children.hasValue(name)) {
                        children.addValue(name);
                    }
                }
                builder.setProperty(children.getPropertyState(true));
            }
        }
    }
}
