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
package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This implementation of a {@link ConflictHandler} always returns the same resolution.
 * It can be used to implement default behaviour or as a base class for more specialised
 * implementations.
 * @deprecated Use {@link org.apache.jackrabbit.oak.plugins.commit.DefaultThreeWayConflictHandler} instead.
 */
@Deprecated
public class DefaultConflictHandler implements ConflictHandler {

    /**
     * A {@code ConflictHandler} which always return {@link org.apache.jackrabbit.oak.spi.commit.ConflictHandler.Resolution#OURS}.
     */
    public static final ConflictHandler OURS = new DefaultConflictHandler(Resolution.OURS);

    /**
     * A {@code ConflictHandler} which always return {@link org.apache.jackrabbit.oak.spi.commit.ConflictHandler.Resolution#THEIRS}.
     */
    public static final ConflictHandler THEIRS = new DefaultConflictHandler(Resolution.THEIRS);

    private final Resolution resolution;

    /**
     * Create a new {@code ConflictHandler} which always returns {@code resolution}.
     *
     * @param resolution  the resolution to return from all methods of this
     * {@code ConflictHandler} instance.
     */
    public DefaultConflictHandler(Resolution resolution) {
        this.resolution = resolution;
    }

    @Override
    public Resolution addExistingProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        return resolution;
    }

    @Override
    public Resolution changeDeletedProperty(NodeBuilder parent, PropertyState ours) {
        return resolution;
    }

    @Override
    public Resolution changeChangedProperty(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        return resolution;
    }

    @Override
    public Resolution deleteChangedProperty(NodeBuilder parent, PropertyState theirs) {
        return resolution;
    }

    @Override
    public Resolution deleteDeletedProperty(NodeBuilder parent, PropertyState ours) {
        return resolution;
    }

    @Override
    public Resolution addExistingNode(NodeBuilder parent, String name, NodeState ours, NodeState theirs) {
        return resolution;
    }

    @Override
    public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours) {
        return resolution;
    }

    @Override
    public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs) {
        return resolution;
    }

    @Override
    public Resolution deleteDeletedNode(NodeBuilder parent, String name) {
        return resolution;
    }
}
