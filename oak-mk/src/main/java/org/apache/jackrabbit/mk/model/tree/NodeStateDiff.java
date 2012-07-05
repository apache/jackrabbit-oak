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
package org.apache.jackrabbit.mk.model.tree;

/**
 * Handler of node state differences.
 * The {@link NodeStore#compare(NodeState, NodeState, NodeStateDiff)} reports
 * detected node state differences by calling methods of a handler instance
 * that implements this interface. The compare method will go through all
 * properties and child nodes of the two states, calling the relevant
 * added, changed or deleted methods where appropriate. Differences in
 * the ordering of properties or child nodes do not affect the comparison,
 * and the order in which such differences are reported is unspecified.
 */
public interface NodeStateDiff {

    /**
     * Called for all added properties.
     *
     * @param after property state after the change
     */
    void propertyAdded(PropertyState after);

    /**
     * Called for all changed properties. The names of the given two
     * property states are guaranteed to be the same.
     *
     * @param before property state before the change
     * @param after property state after the change
     */
    void propertyChanged(PropertyState before, PropertyState after);

    /**
     * Called for all deleted properties.
     *
     * @param before property state before the change
     */
    void propertyDeleted(PropertyState before);

    /**
     * Called for all added child nodes.
     *
     * @param name name of the added child node
     * @param after child node state after the change
     */
    void childNodeAdded(String name, NodeState after);

    /**
     * Called for all changed child nodes.
     *
     * @param name name of the changed child node
     * @param before child node state before the change
     * @param after child node state after the change
     */
    void childNodeChanged(String name, NodeState before, NodeState after);

    /**
     * Called for all deleted child nodes.
     *
     * @param name name of the deleted child node
     * @param before child node state before the change
     */
    void childNodeDeleted(String name, NodeState before);

}
