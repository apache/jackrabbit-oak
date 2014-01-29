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
package org.apache.jackrabbit.oak.jcr.observation;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.namepath.PathTracker;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierTracker;
import org.apache.jackrabbit.oak.plugins.observation.EventHandler;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Change handler that generates JCR Event instances and places them
 * in an event queue.
 */
class QueueingHandler implements EventHandler {

    private final QueueingHandler parent;

    private final String name;

    private final PathTracker pathTracker;

    private final IdentifierTracker identifierTracker;

    private final EventQueue queue;

    private final EventFactory factory;

    private final NodeState before;

    private final NodeState after;

    QueueingHandler(
            EventQueue queue, EventFactory factory,
            NodeState before, NodeState after) {
        this.parent = null;
        this.name = null;

        this.pathTracker = new PathTracker();
        if (after.exists()) {
            this.identifierTracker = new IdentifierTracker(after);
        } else {
            this.identifierTracker = new IdentifierTracker(before);
        }

        this.queue = queue;
        this.factory = factory;

        this.before = before;
        this.after = after;
    }

    private QueueingHandler(
            QueueingHandler parent,
            String name, NodeState before, NodeState after) {
        this.parent = parent;
        this.name = name;

        this.pathTracker = parent.pathTracker.getChildTracker(name);
        if (after.exists()) {
            this.identifierTracker =
                    parent.identifierTracker.getChildTracker(name, after);
        } else {
            this.identifierTracker =
                    parent.getBeforeIdentifierTracker().getChildTracker(name, before);
        }

        this.queue = parent.queue;
        this.factory = parent.factory;

        this.before = before;
        this.after = after;
    }

    private IdentifierTracker getBeforeIdentifierTracker() {
        if (!after.exists()) {
            return identifierTracker;
        } else if (parent != null) {
            return parent.getBeforeIdentifierTracker().getChildTracker(name, before);
        } else {
            return new IdentifierTracker(before);
        }
    }

    //-----------------------------------------------------< ChangeHandler >--

    @Override
    public EventHandler getChildHandler(
            String name, NodeState before, NodeState after) {
        return new QueueingHandler(this, name, before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) {
        queue.addEvent(factory.propertyAdded(
                pathTracker.getPath(), after.getName(),
                identifierTracker.getIdentifier()));
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        queue.addEvent(factory.propertyChanged(
                pathTracker.getPath(), after.getName(),
                identifierTracker.getIdentifier()));
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        queue.addEvent(factory.propertyDeleted(
                pathTracker.getPath(), before.getName(),
                identifierTracker.getIdentifier()));
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        queue.addEvent(factory.nodeAdded(
                pathTracker.getPath(), name,
                identifierTracker.getChildTracker(name, after).getIdentifier()));
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        queue.addEvent(factory.nodeDeleted(
                pathTracker.getPath(), name,
                getBeforeIdentifierTracker().getChildTracker(name, before).getIdentifier()));
    }

    @Override
    public void nodeMoved(
            final String sourcePath, String name, NodeState moved) {
        queue.addEvent(factory.nodeMoved(
                pathTracker.getPath(), name,
                identifierTracker.getChildTracker(name, moved).getIdentifier(),
                sourcePath));
    }

    @Override
    public void nodeReordered(
            final String destName, final String name, NodeState reordered) {
        queue.addEvent(factory.nodeReordered(
                pathTracker.getPath(), name,
                identifierTracker.getChildTracker(name, reordered).getIdentifier(),
                destName));
    }

}
