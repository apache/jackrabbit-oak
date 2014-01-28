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

import javax.jcr.observation.Event;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code EventTypeFilter} filters based on event types as defined
 * by {@link javax.jcr.observation.ObservationManager#addEventListener(
        javax.jcr.observation.EventListener, int, String, boolean, String[], String[], boolean)
        ObservationManager.addEventListener()}.
 */
public class EventTypeFilter implements EventFilter {
    private final int eventTypes;

    /**
     * Create a new {@code Filter} instance that includes all events matching
     * the {@code eventTypes} bit mask. That is, a given event is included if
     * the corresponding bit in {@code eventType} is set.
     *
     * @param eventTypes  bit mask encoding the types of events to include
     *
     * @see Event
     */
    public EventTypeFilter(int eventTypes) {
        this.eventTypes = eventTypes;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return includeByEvent(Event.PROPERTY_ADDED);
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return includeByEvent(Event.PROPERTY_CHANGED);
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return includeByEvent(Event.PROPERTY_REMOVED);
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return includeByEvent(Event.NODE_ADDED);
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return includeByEvent(Event.NODE_REMOVED);
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        return includeByEvent(Event.NODE_MOVED);
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        return includeByEvent(Event.NODE_MOVED);
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        return this;
    }

    //------------------------------------------------------------< internal >---

    private boolean includeByEvent(int eventType) {
        return (this.eventTypes & eventType) != 0;
    }

}
