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

import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.namepath.PathTracker;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierTracker;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.observation.DefaultEventHandler;
import org.apache.jackrabbit.oak.plugins.observation.EventHandler;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventAggregator;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Event handler that uses the given {@link EventFactory} and tracked path
 * and identifier information to translate change callbacks to corresponding
 * JCR events that are then placed in the given {@link EventQueue}.
 */
class QueueingHandler extends DefaultEventHandler {
    
    class AggregationResult {
        
        private final String name;
        private final IdentifierTracker identifierTracker;
        private final String primaryType;
        private final Iterable<String> mixinTypes;
        private final PathTracker pathTracker;

        AggregationResult(String name, IdentifierTracker identifierTracker,
                final String primaryType, final Iterable<String> mixinTypes, PathTracker pathTracker) {
            this.name = name;
            this.identifierTracker = identifierTracker;
            this.primaryType = primaryType;
            this.mixinTypes = mixinTypes;
            this.pathTracker = pathTracker;
        }
    }
    
    private final QueueingHandler parent;

    private final EventQueue queue;

    private final EventFactory factory;

    private final PathTracker pathTracker;

    private final String parentType;

    private final Iterable<String> parentMixins;

    // need to track identifiers for both before and after trees,
    // to get correct identifiers for events in removed subtrees
    private final IdentifierTracker beforeIdentifierTracker;

    private final IdentifierTracker identifierTracker;

    private final EventAggregator aggregator;

    private final String name;
    
    private final NodeState root;

    private final List<ChildNodeEntry> parents;
    
    QueueingHandler(
            EventQueue queue, EventFactory factory,
            EventAggregator aggregator, NodeState before, NodeState after) {
        this.parent = null;
        this.queue = queue;
        this.factory = factory;
        this.name = null;
        this.aggregator = aggregator;
        this.pathTracker = new PathTracker();
        this.beforeIdentifierTracker = new IdentifierTracker(before);
        this.parents = new LinkedList<ChildNodeEntry>();
        if (after.exists()) {
            this.identifierTracker = new IdentifierTracker(after);
            this.parentType = getPrimaryType(after);
            this.parentMixins = getMixinTypes(after);
            this.root = after;
        } else {
            this.identifierTracker = beforeIdentifierTracker;
            this.parentType = getPrimaryType(before);
            this.parentMixins = getMixinTypes(before);
            this.root = before;
        }
    }

    private QueueingHandler(
            QueueingHandler parent,
            String name, NodeState before, NodeState after) {
        this.parent = parent;
        this.queue = parent.queue;
        this.factory = parent.factory;
        this.root = parent.root;
        this.name = name;
        this.aggregator = parent.aggregator;
        this.pathTracker = parent.pathTracker.getChildTracker(name);
        this.beforeIdentifierTracker =
                parent.beforeIdentifierTracker.getChildTracker(name, before);
        this.parents = new LinkedList<ChildNodeEntry>(parent.parents);
        if (after.exists()) {
            this.identifierTracker =
                    parent.identifierTracker.getChildTracker(name, after);
            this.parentType = getPrimaryType(after);
            this.parentMixins = getMixinTypes(after);
            this.parents.add(new MemoryChildNodeEntry(name, after));
        } else {
            this.identifierTracker = beforeIdentifierTracker;
            this.parentType = getPrimaryType(before);
            this.parentMixins = getMixinTypes(before);
            this.parents.add(new MemoryChildNodeEntry(name, before));
        }
    }

    //-----------------------------------------------------< ChangeHandler >--

    @Override
    public EventHandler getChildHandler(
            String name, NodeState before, NodeState after) {
        return new QueueingHandler(this, name, before, after);
    }

    private AggregationResult aggregate(PropertyState after) {
        int aggregationLevel = 0;
        if (aggregator != null) {
            aggregationLevel = aggregator.aggregate(root, parents, after);
        }
        if (aggregationLevel <= 0) {
            // no aggregation
            return new AggregationResult(after.getName(), this.identifierTracker, parentType, parentMixins, pathTracker);
        } else {
            QueueingHandler handler = this;
            String name = after.getName();
            for(int i=0; i<aggregationLevel; i++) {
                name = handler.name + "/" + name;
                handler = handler.parent;
            }
            return new AggregationResult(name, handler.identifierTracker, handler.parentType, handler.parentMixins, handler.pathTracker);
        }
    }

    @Override
    public void propertyAdded(PropertyState after) {
        AggregationResult aggregated = aggregate(after);
        queue.addEvent(factory.propertyAdded(
                after,
                aggregated.primaryType, aggregated.mixinTypes,
                aggregated.pathTracker.getPath(), aggregated.name,
                aggregated.identifierTracker.getIdentifier()));
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        AggregationResult aggregated = aggregate(after);
        queue.addEvent(factory.propertyChanged(
                before, after,
                aggregated.primaryType, aggregated.mixinTypes,
                aggregated.pathTracker.getPath(), aggregated.name,
                aggregated.identifierTracker.getIdentifier()));
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        AggregationResult aggregated = aggregate(before);
        queue.addEvent(factory.propertyDeleted(
                before,
                aggregated.primaryType, aggregated.mixinTypes,
                aggregated.pathTracker.getPath(), aggregated.name,
                aggregated.identifierTracker.getIdentifier()));
    }

    private AggregationResult aggregate(String name, NodeState node, IdentifierTracker childTracker) {
        int aggregationLevel = 0;
        if (aggregator != null) {
            aggregationLevel = aggregator.aggregate(root, parents, new MemoryChildNodeEntry(name, node));
        }
        if (aggregationLevel <= 0) {
            // no aggregation
            return new AggregationResult(name, childTracker, getPrimaryType(node), getMixinTypes(node), pathTracker);
        } else {
            QueueingHandler handler = this;
            IdentifierTracker tracker = childTracker;
            String primaryType = null;
            Iterable<String> mixinTypes = null;
            PathTracker pathTracker = null;
            String childName = null;
            for(int i=0; i<aggregationLevel; i++) {
                if (i > 0) {
                    name = childName + "/" + name;
                }
                tracker = handler.identifierTracker;
                primaryType = handler.parentType;
                mixinTypes = handler.parentMixins;
                pathTracker = handler.pathTracker;
                childName = handler.name;
                handler = handler.parent;
            }
            return new AggregationResult(name, tracker, primaryType, mixinTypes, pathTracker);
        }
    }

    @Override
    public void nodeAdded(String name, NodeState after) {
        IdentifierTracker tracker =
                identifierTracker.getChildTracker(name, after);
        AggregationResult aggregated = aggregate(name, after, tracker);
        queue.addEvent(factory.nodeAdded(
                aggregated.primaryType, aggregated.mixinTypes,
                aggregated.pathTracker.getPath(), aggregated.name, aggregated.identifierTracker.getIdentifier()));
    }

    @Override
    public void nodeDeleted(String name, NodeState before) {
        IdentifierTracker tracker =
                beforeIdentifierTracker.getChildTracker(name, before);
        AggregationResult aggregated = aggregate(name, before, tracker);
        queue.addEvent(factory.nodeDeleted(
                aggregated.primaryType, aggregated.mixinTypes,
                aggregated.pathTracker.getPath(), aggregated.name, aggregated.identifierTracker.getIdentifier()));
    }

    @Override
    public void nodeMoved(
            final String sourcePath, String name, NodeState moved) {
        IdentifierTracker tracker =
                identifierTracker.getChildTracker(name, moved);
        AggregationResult aggregated = aggregate(name, moved, tracker);
        queue.addEvent(factory.nodeMoved(
                aggregated.primaryType, aggregated.mixinTypes,
                aggregated.pathTracker.getPath(), aggregated.name, aggregated.identifierTracker.getIdentifier(),
                sourcePath));
    }

    @Override
    public void nodeReordered(
            final String destName, final String name, NodeState reordered) {
        IdentifierTracker tracker =
                identifierTracker.getChildTracker(name, reordered);
        AggregationResult aggregated = aggregate(name, reordered, tracker);
        queue.addEvent(factory.nodeReordered(
                aggregated.primaryType, aggregated.mixinTypes,
                aggregated.pathTracker.getPath(), aggregated.name, aggregated.identifierTracker.getIdentifier(),
                destName));
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

}
