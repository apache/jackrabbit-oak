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
package org.apache.jackrabbit.mk.model;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.model.ChildNodeEntry;
import org.apache.jackrabbit.oak.model.NodeState;
import org.apache.jackrabbit.oak.model.PropertyState;

/**
 * Utility base class for comparing two {@link NodeState} instances. The
 * {@link #compare(NodeState, NodeState)} method will go through all
 * properties and child nodes of the two states, calling the relevant
 * added, changed or deleted methods where appropriate. Differences in
 * the ordering of properties or child nodes do not affect the comparison.
 */
public class NodeStateDiff {

    /**
     * Called by {@link #compare(NodeState, NodeState)} for all added
     * properties. The default implementation does nothing.
     *
     * @param after property state after the change
     */
    public void propertyAdded(PropertyState after) {
    }

    /**
     * Called by {@link #compare(NodeState, NodeState)} for all changed
     * properties. The names of the given two property states are guaranteed
     * to be the same. The default implementation does nothing.
     *
     * @param before property state before the change
     * @param after property state after the change
     */
    public void propertyChanged(PropertyState before, PropertyState after) {
    }

    /**
     * Called by {@link #compare(NodeState, NodeState)} for all deleted
     * properties. The default implementation does nothing.
     *
     * @param before property state before the change
     */
    public void propertyDeleted(PropertyState before) {
    }

    /**
     * Called by {@link #compare(NodeState, NodeState)} for all added
     * child nodes. The default implementation does nothing.
     *
     * @param name name of the added child node
     * @param after child node state after the change
     */
    public void childNodeAdded(String name, NodeState after) {
    }

    /**
     * Called by {@link #compare(NodeState, NodeState)} for all changed
     * child nodes. The default implementation does nothing.
     *
     * @param name name of the changed child node
     * @param before child node state before the change
     * @param after child node state after the change
     */
    public void childNodeChanged(String name, NodeState before, NodeState after) {
    }

    /**
     * Called by {@link #compare(NodeState, NodeState)} for all deleted
     * child nodes. The default implementation does nothing.
     *
     * @param name name of the deleted child node
     * @param before child node state before the change
     */
    public void childNodeDeleted(String name, NodeState before) {
    }

    /**
     * Compares the given two node states. Any found differences are
     * reported by calling the relevant added, changed or deleted methods.
     *
     * @param before node state before changes
     * @param after node state after changes
     */
    public void compare(NodeState before, NodeState after) {
        compareProperties(before, after);
        compareChildNodes(before, after);
    }

    /**
     * Compares the properties of the given two node states.
     *
     * @param before node state before changes
     * @param after node state after changes
     */
    private void compareProperties(NodeState before, NodeState after) {
        Set<String> beforeProperties = new HashSet<String>();

        for (PropertyState beforeProperty : before.getProperties()) {
            String name = beforeProperty.getName();
            PropertyState afterProperty = after.getProperty(name);
            if (afterProperty == null) {
                propertyDeleted(beforeProperty);
            } else {
                beforeProperties.add(name);
                if (!beforeProperty.equals(afterProperty)) {
                    propertyChanged(beforeProperty, afterProperty);
                }
            }
        }

        for (PropertyState afterProperty : after.getProperties()) {
            if (!beforeProperties.contains(afterProperty.getName())) {
                propertyAdded(afterProperty);
            }
        }
    }

    /**
     * Compares the child nodes of the given two node states.
     *
     * @param before node state before changes
     * @param after node state after changes
     */
    private void compareChildNodes(NodeState before, NodeState after) {
        Set<String> beforeChildNodes = new HashSet<String>();

        for (ChildNodeEntry beforeCNE : before.getChildNodeEntries(0, -1)) {
            String name = beforeCNE.getName();
            NodeState beforeChild = beforeCNE.getNode();
            NodeState afterChild = after.getChildNode(name);
            if (afterChild == null) {
                childNodeDeleted(name, beforeChild);
            } else {
                beforeChildNodes.add(name);
                if (!beforeChild.equals(afterChild)) {
                    childNodeChanged(name, beforeChild, afterChild);
                }
            }
        }

        for (ChildNodeEntry afterChild : after.getChildNodeEntries(0, -1)) {
            String name = afterChild.getName();
            if (!beforeChildNodes.contains(name)) {
                childNodeAdded(name, afterChild.getNode());
            }
        }
    }

}
