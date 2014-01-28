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

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Filter for determining what changes to report the the event listener.
 */
public interface EventFilter {

    /**
     * Include an added property
     * @param after  added property
     * @return  {@code true} if the property should be included
     */
    boolean includeAdd(PropertyState after);

    /**
     * Include a changed property
     * @param before  property before the change
     * @param after  property after the change
     * @return  {@code true} if the property should be included
     */
    boolean includeChange(PropertyState before, PropertyState after);

    /**
     * Include a deleted property
     * @param before  deleted property
     * @return  {@code true} if the property should be included
     */
    boolean includeDelete(PropertyState before);

    /**
     * Include an added node
     * @param name name of the node
     * @param after  added node
     * @return  {@code true} if the node should be included
     */
    boolean includeAdd(String name, NodeState after);

    /**
     * Include a deleted node
     * @param name name of the node
     * @param before deleted node
     * @return  {@code true} if the node should be included
     */
    boolean includeDelete(String name, NodeState before);

    /**
     * Include a moved node
     * @param sourcePath  source path of the move operation
     * @param name        name of the moved node
     * @param moved       the moved node
     * @return  {@code true} if the node should be included
     */
    boolean includeMove(String sourcePath, String name, NodeState moved);

    /**
     * Include a reordered node
     * @param destName    name of the {@code orderBefore()} destination node
     * @param name        name of the reordered node
     * @param reordered   the reordered node
     * @return  {@code true} if the node should be included
     */
    boolean includeReorder(String destName, String name, NodeState reordered);

    /**
     * Factory for creating a filter instance for the given child node
     * @param name  name of the child node
     * @param before  before state of the child node
     * @param after  after state of the child node
     * @return  filter instance for filtering the child node or {@code null} to
     *          exclude the sub tree rooted at this child node.
     */
    @CheckForNull
    EventFilter create(String name, NodeState before, NodeState after);
}