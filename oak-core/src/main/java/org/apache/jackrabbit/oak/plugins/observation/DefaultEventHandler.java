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

package org.apache.jackrabbit.oak.plugins.observation;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Default implementation of {@code EventHandler} that
 * does nothing.
 */
public class DefaultEventHandler implements EventHandler {
    public static EventHandler INSTANCE = new DefaultEventHandler();

    @Override
    public void enter(NodeState before, NodeState after) {
        // do nothing
    }

    @Override
    public void leave(NodeState before, NodeState after) {
        // do nothing
    }

    /**
     * @return  {@code this}
     */
    @Override
    public EventHandler getChildHandler(String name, NodeState before, NodeState after) {
        return this;
    }

    @Override
    public void propertyAdded(PropertyState after) {
        // do nothing
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        // do nothing
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        // do nothing
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        // do nothing
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        // do nothing
    }

    @Override
    public void nodeMoved(String sourcePath, String name, NodeState moved) {
        // do nothing
    }

    @Override
    public void nodeReordered(String destName, String name, NodeState reordered) {
        // do nothing
    }
}
