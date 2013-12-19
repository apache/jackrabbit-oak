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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyMap;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.core.AbstractTree.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.getIdentifier;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jcr.observation.Event;

import com.google.common.collect.ImmutableMap;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.PathMapper;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventIterator.IterableListener;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO JcrListener...
 */
class JcrListener implements IterableListener<Event> {
    private final Tree beforeTree;
    private final Tree afterTree;
    private final List<Event> events = newArrayList();
    private final PathMapper namePathMapper;
    private final String userId;
    private final long timestamp;
    private final String message;
    private final boolean external;

    JcrListener(Tree beforeTree, Tree afterTree, PathMapper namePathMapper, CommitInfo info) {
        this.beforeTree = beforeTree;
        this.afterTree = afterTree;
        this.namePathMapper = namePathMapper;
        if (info != null) {
            this.userId = info.getUserId();
            this.message = info.getMessage();
            this.timestamp = info.getDate();
            this.external = false;
        } else {
            this.userId = CommitInfo.OAK_UNKNOWN;
            this.message = null;
            // we can't tell exactly when external changes were committed,
            // so we just use a rough estimate like this
            this.timestamp = System.currentTimeMillis();
            this.external = true;
        }
    }

    private JcrListener(Tree beforeTree, Tree afterTree, PathMapper namePathMapper, String userId,
            long timestamp, String message, boolean external) {
        this.beforeTree = beforeTree;
        this.afterTree = afterTree;
        this.namePathMapper = namePathMapper;
        this.userId = userId;
        this.timestamp = timestamp;
        this.message = message;
        this.external = external;
    }

    @Override
    public void propertyAdded(PropertyState after) {
        events.add(createEvent(PROPERTY_ADDED, afterTree, after));
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        events.add(createEvent(Event.PROPERTY_CHANGED, afterTree, after));
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        events.add(createEvent(PROPERTY_REMOVED, beforeTree, before));
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        events.add(createEvent(NODE_ADDED, afterTree.getChild(name)));
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        detectReorder(name, before, after);
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        events.add(createEvent(NODE_REMOVED, beforeTree.getChild(name)));
    }

    @Override
    public void nodeMoved(String sourcePath, String name, NodeState moved) {
        String destPath = PathUtils.concat(afterTree.getPath(), name);
        events.add(createEvent(NODE_MOVED, afterTree.getChild(name),
                ImmutableMap.of(
                        "srcAbsPath", namePathMapper.getJcrPath(sourcePath),
                        "destAbsPath", namePathMapper.getJcrPath(destPath))));
    }

    @Override
    public JcrListener create(String name, NodeState before, NodeState after) {
        return new JcrListener(beforeTree.getChild(name), afterTree.getChild(name), namePathMapper,
                userId, timestamp, message, external);
    }

    @Override
    public Iterator<Event> iterator() {
        return events.iterator();
    }

    //------------------------------------------------------------< private >---

    private void detectReorder(String name, NodeState before, NodeState after) {
        List<String> afterNames = newArrayList(after.getNames(OAK_CHILD_ORDER));
        List<String> beforeNames = newArrayList(before.getNames(OAK_CHILD_ORDER));

        afterNames.retainAll(beforeNames);
        beforeNames.retainAll(afterNames);

        // Selection sort beforeNames into afterNames recording the swaps as we go
        for (int a = 0; a < afterNames.size(); a++) {
            String afterName = afterNames.get(a);
            for (int b = a; b < beforeNames.size(); b++) {
                String beforeName = beforeNames.get(b);
                if (a != b && beforeName.equals(afterName)) {
                    beforeNames.set(b, beforeNames.get(a));
                    beforeNames.set(a, beforeName);
                    events.add(createEvent(NODE_MOVED, afterTree.getChild(name).getChild(afterName),
                            ImmutableMap.of(
                                    "srcChildRelPath", beforeNames.get(a),
                                    "destChildRelPath", beforeNames.get(a + 1))));
                }
            }
        }
    }

    private Event createEvent(int eventType, Tree tree) {
        return createEvent(eventType, tree.getPath(), getIdentifier(tree), emptyMap());
    }

    private Event createEvent(int eventType, Tree tree, Map<?, ?> info) {
        return createEvent(eventType, tree.getPath(), getIdentifier(tree), info);
    }

    private Event createEvent(int eventType, Tree parent, PropertyState property) {
        String path = PathUtils.concat(parent.getPath(), property.getName());
        return createEvent(eventType, path, getIdentifier(parent), emptyMap());
    }

    private Event createEvent(int eventType, String path, String id, Map<?, ?> info) {
        return new EventImpl(
                eventType, namePathMapper.getJcrPath(path), userId, id,
                info, timestamp, message, external);
    }

}
