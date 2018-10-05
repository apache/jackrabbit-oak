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
package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * Handler of node state differences. The
 * {@link NodeState#compareAgainstBaseState(NodeState, NodeStateDiff)}
 * method reports detected node state differences by calling methods of
 * a handler instance that implements this interface.
 * <p>
 * The compare method goes through all properties and child nodes of the
 * two states, calling the relevant added, changed or deleted methods
 * where appropriate. Differences in the ordering of properties or child
 * nodes do not affect the comparison, and the order in which such
 * differences are reported is unspecified.
 * <p>
 * The methods in this interface all return a boolean value to signify whether
 * the comparison should continue or abort. If a method returns {@code false},
 * then the comparison is immediately stopped, that means sibling nodes and
 * sibling nodes of all parents are not further processed. Otherwise it
 * continues until all changes have been reported.
 * <p>
 * Note that the
 * {@link NodeState#compareAgainstBaseState(NodeState, NodeStateDiff)}
 * method only compares the given states without recursing to the subtrees
 * below. An implementation of this interface should recursively call that
 * method for the relevant child node entries to find out all the changes
 * across the entire subtree below the given node.
 */
public interface NodeStateDiff {

    /**
     * Called for all added properties.
     *
     * @param after property state after the change
     * @return {@code true} to continue the comparison, {@code false} to abort.
     *         Abort will stop comparing completely, that means sibling nodes
     *         and sibling nodes of all parents are not further compared.
     */
    boolean propertyAdded(PropertyState after);

    /**
     * Called for all changed properties. The names of the given two
     * property states are guaranteed to be the same.
     *
     * @param before property state before the change
     * @param after property state after the change
     * @return {@code true} to continue the comparison, {@code false} to abort.
     *         Abort will stop comparing completely, that means sibling nodes
     *         and sibling nodes of all parents are not further compared.
     */
    boolean propertyChanged(PropertyState before, PropertyState after);

    /**
     * Called for all deleted properties.
     *
     * @param before property state before the change
     * @return {@code true} to continue the comparison, {@code false} to abort.
     *         Abort will stop comparing completely, that means sibling nodes
     *         and sibling nodes of all parents are not further compared.
     */
    boolean propertyDeleted(PropertyState before);

    /**
     * Called for all added child nodes.
     *
     * @param name name of the added child node
     * @param after child node state after the change
     * @return {@code true} to continue the comparison, {@code false} to abort.
     *         Abort will stop comparing completely, that means sibling nodes
     *         and sibling nodes of all parents are not further compared.
     */
    boolean childNodeAdded(String name, NodeState after);

    /**
     * Called for all child nodes that may contain changes between the before
     * and after states. The comparison implementation is expected to make an
     * effort to avoid calling this method on child nodes under which nothing
     * has changed.
     *
     * @param name name of the changed child node
     * @param before child node state before the change
     * @param after child node state after the change
     * @return {@code true} to continue the comparison, {@code false} to abort.
     *         Abort will stop comparing completely, that means sibling nodes
     *         and sibling nodes of all parents are not further compared.
     */
    boolean childNodeChanged(String name, NodeState before, NodeState after);

    /**
     * Called for all deleted child nodes.
     *
     * @param name name of the deleted child node
     * @param before child node state before the change
     * @return {@code true} to continue the comparison, {@code false} to abort.
     *         Abort will stop comparing completely, that means sibling nodes
     *         and sibling nodes of all parents are not further compared.
     */
    boolean childNodeDeleted(String name, NodeState before);

}
