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
package org.apache.jackrabbit.oak.plugins.observation.handler;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Filtered change handler. This decorator class applies an {@link EventFilter}
 * on all detected changes, and forwards the filtered changes to a given
 * delegate change handler.
 */
public class FilteredHandler implements ChangeHandler {

    private final EventFilter filter;

    private final ChangeHandler handler;

    public FilteredHandler(EventFilter filter, ChangeHandler handler) {
        this.filter = filter;
        this.handler = handler;
    }

    @Override @CheckForNull
    public ChangeHandler getChildHandler(
            String name, NodeState before, NodeState after) {
        EventFilter f = filter.create(name, before, after);
        if (f != null) {
            ChangeHandler h = handler.getChildHandler(name, before, after);
            if (h != null) { 
                return new FilteredHandler(f, h);
            }
        }
        return null;
    }

    @Override
    public void propertyAdded(PropertyState after) {
        if (filter.includeAdd(after)) {
            handler.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        if (filter.includeChange(before, after)) {
            handler.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        if (filter.includeDelete(before)) {
            handler.propertyDeleted(before);
        }
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        if (filter.includeAdd(name, after)) {
            handler.nodeAdded(name, after);
        }
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        if (filter.includeDelete(name, before)) {
            handler.nodeDeleted(name, before);
        }
    }

    @Override
    public void nodeMoved(String sourcePath, String name, NodeState moved) {
        if (filter.includeMove(sourcePath, name, moved)) {
            handler.nodeMoved(sourcePath, name, moved);
        }
    }

    @Override
    public void nodeReordered(String destName, String name, NodeState reordered) {
        if (filter.includeReorder(destName, name, reordered)) {
            handler.nodeReordered(destName, name, reordered);
        }
    }

}
