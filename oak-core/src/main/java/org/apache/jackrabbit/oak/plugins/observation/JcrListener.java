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
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.core.AbstractTree.OAK_CHILD_ORDER;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jcr.observation.Event;

import com.google.common.collect.ImmutableMap;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.EventIterable.IterableListener;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * TODO JcrListener...
 */
class JcrListener implements IterableListener<Event> {

    private final EventContext context;

    private final ImmutableTree beforeTree;
    private final ImmutableTree afterTree;

    private final List<Event> events = newArrayList();

    JcrListener(
            EventContext context,
            ImmutableTree beforeTree, ImmutableTree afterTree) {
        this.context = context;
        this.beforeTree = beforeTree;
        this.afterTree = afterTree;
    }

    @Override
    public void propertyAdded(PropertyState after) {
        events.add(new EventImpl(
                context, PROPERTY_ADDED, afterTree, after.getName()));
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        events.add(new EventImpl(
                context, PROPERTY_CHANGED, afterTree, after.getName()));
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        events.add(new EventImpl(
                context, PROPERTY_REMOVED, beforeTree, before.getName()));
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        events.add(new EventImpl(
                context, NODE_ADDED, new ImmutableTree(afterTree, name, after)));
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        detectReorder(name, before, after);
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        events.add(new EventImpl(
                context, NODE_REMOVED, new ImmutableTree(beforeTree, name, before)));
    }

    @Override
    public void nodeMoved(String sourcePath, String name, NodeState moved) {
        String destPath = PathUtils.concat(afterTree.getPath(), name);
        ImmutableMap<String, String> info = ImmutableMap.of(
                "srcAbsPath", context.getJcrPath(sourcePath),
                "destAbsPath", context.getJcrPath(destPath));
        ImmutableTree tree = new ImmutableTree(afterTree, name, moved);
        events.add(new EventImpl(context, NODE_MOVED, tree, info));
    }

    @Override
    public JcrListener create(String name, NodeState before, NodeState after) {
        return new JcrListener(
                context,
                new ImmutableTree(beforeTree, name, before),
                new ImmutableTree(afterTree, name, after));
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

        ImmutableTree parent = new ImmutableTree(afterTree, name, after);
        // Selection sort beforeNames into afterNames recording the swaps as we go
        for (int a = 0; a < afterNames.size(); a++) {
            String afterName = afterNames.get(a);
            for (int b = a; b < beforeNames.size(); b++) {
                String beforeName = beforeNames.get(b);
                if (a != b && beforeName.equals(afterName)) {
                    beforeNames.set(b, beforeNames.get(a));
                    beforeNames.set(a, beforeName);
                    Map<String, String> info = ImmutableMap.of(
                            "srcChildRelPath", context.getJcrName(beforeNames.get(a)),
                            "destChildRelPath", context.getJcrName(beforeNames.get(a + 1)));
                    ImmutableTree tree = parent.getChild(afterName);
                    events.add(new EventImpl(context, NODE_MOVED, tree, info));
                }
            }
        }
    }

}
