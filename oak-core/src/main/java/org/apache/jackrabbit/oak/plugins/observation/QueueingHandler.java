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

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;

import java.util.LinkedList;
import java.util.Map;

import javax.jcr.observation.Event;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.handler.ChangeHandler;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableMap;

/**
 * Change handler that generates JCR Event instances and places them
 * in an event queue.
 */
class QueueingHandler implements ChangeHandler {

    private final LinkedList<Event> queue;

    private final EventContext context;

    private final ImmutableTree before;

    private final ImmutableTree after;

    QueueingHandler(
            LinkedList<Event> queue, EventContext context,
            ImmutableTree before, ImmutableTree after) {
        this.queue = queue;
        this.context = context;
        this.before = before;
        this.after = after;
    }

    @Override
    public ChangeHandler getChildHandler(
            String name, NodeState before, NodeState after) {
        return new QueueingHandler(
                queue, context,
                new ImmutableTree(this.before, name, before),
                new ImmutableTree(this.after, name, after));
    }

    @Override
    public void propertyAdded(PropertyState after) {
        queue.add(new EventImpl(
                context, PROPERTY_ADDED, this.after, after.getName()));
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        queue.add(new EventImpl(
                context, PROPERTY_CHANGED, this.after, after.getName()));
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        queue.add(new EventImpl(
                context, PROPERTY_REMOVED, this.before, before.getName()));
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        ImmutableTree tree = new ImmutableTree(this.after, name, after);
        queue.add(new EventImpl(context, NODE_ADDED, tree));
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        ImmutableTree tree = new ImmutableTree(this.before, name, before);
        queue.add(new EventImpl(context, NODE_REMOVED, tree));
    }

    @Override
    public void nodeMoved(String sourcePath, String name, NodeState moved) {
        ImmutableTree tree = new ImmutableTree(this.after, name, moved);
        Map<String, String> info = ImmutableMap.of(
                "srcAbsPath", context.getJcrPath(sourcePath),
                "destAbsPath", context.getJcrPath(tree.getPath()));
        queue.add(new EventImpl(context, NODE_MOVED, tree, info));
    }

    @Override
    public void nodeReordered(
            String destName, String name, NodeState reordered) {
        Map<String, String> info = ImmutableMap.of(
                "srcChildRelPath", context.getJcrName(name),
                "destChildRelPath", context.getJcrName(destName));
        ImmutableTree tree = new ImmutableTree(after, name, reordered);
        queue.add(new EventImpl(context, NODE_MOVED, tree, info));
    }

}
