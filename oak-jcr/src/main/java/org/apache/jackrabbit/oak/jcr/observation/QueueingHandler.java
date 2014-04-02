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

import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.namepath.PathTracker;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierTracker;
import org.apache.jackrabbit.oak.plugins.observation.DefaultEventHandler;
import org.apache.jackrabbit.oak.plugins.observation.EventHandler;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Event handler that uses the given {@link EventFactory} and tracked path
 * and identifier information to translate change callbacks to corresponding
 * JCR events that are then placed in the given {@link EventQueue}.
 */
class QueueingHandler extends DefaultEventHandler {

    private final EventQueue queue;

    private final EventFactory factory;

    private final PathTracker pathTracker;

    // need to track identifiers for both before and after trees,
    // to get correct identifiers for events in removed subtrees
    private final IdentifierTracker beforeIdentifierTracker;

    private final IdentifierTracker identifierTracker;

    QueueingHandler(
            EventQueue queue, EventFactory factory,
            NodeState before, NodeState after) {
        this.queue = queue;
        this.factory = factory;
        this.pathTracker = new PathTracker();
        this.beforeIdentifierTracker = new IdentifierTracker(before);
        if (after.exists()) {
            this.identifierTracker = new IdentifierTracker(after);
        } else {
            this.identifierTracker = beforeIdentifierTracker;
        }
    }

    private QueueingHandler(
            QueueingHandler parent,
            String name, NodeState before, NodeState after) {
        this.queue = parent.queue;
        this.factory = parent.factory;
        this.pathTracker = parent.pathTracker.getChildTracker(name);
        this.beforeIdentifierTracker =
                parent.beforeIdentifierTracker.getChildTracker(name, before);
        if (after.exists()) {
            this.identifierTracker =
                    parent.identifierTracker.getChildTracker(name, after);
        } else {
            this.identifierTracker = beforeIdentifierTracker;
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

    private static String getPrimaryType(NodeState before) {
        PropertyState primaryType = before.getProperty(JCR_PRIMARYTYPE);
        if (primaryType != null && primaryType.getType() == NAME) {
            return primaryType.getValue(NAME);
        } else {
            return null;
        }
    }

    private static Iterable<String> getMixinTypes(NodeState before) {
        PropertyState mixinTypes = before.getProperty(JCR_MIXINTYPES);
        if (mixinTypes != null && mixinTypes.getType() == NAMES) {
            return mixinTypes.getValue(NAMES);
        } else {
            return emptyList();
        }
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        IdentifierTracker tracker =
                identifierTracker.getChildTracker(name, after);
        queue.addEvent(factory.nodeAdded(
                getPrimaryType(after), getMixinTypes(after),
                pathTracker.getPath(), name, tracker.getIdentifier()));
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        IdentifierTracker tracker =
                beforeIdentifierTracker.getChildTracker(name, before);

        queue.addEvent(factory.nodeDeleted(
                getPrimaryType(before), getMixinTypes(before),
                pathTracker.getPath(), name, tracker.getIdentifier()));
    }

    @Override
    public void nodeMoved(
            final String sourcePath, String name, NodeState moved) {
        IdentifierTracker tracker =
                identifierTracker.getChildTracker(name, moved);
        queue.addEvent(factory.nodeMoved(
                pathTracker.getPath(), name, tracker.getIdentifier(),
                sourcePath));
    }

    @Override
    public void nodeReordered(
            final String destName, final String name, NodeState reordered) {
        IdentifierTracker tracker =
                identifierTracker.getChildTracker(name, reordered);
        queue.addEvent(factory.nodeReordered(
                pathTracker.getPath(), name, tracker.getIdentifier(),
                destName));
    }

}
