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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.model.AbstractChildNodeEntry;
import org.apache.jackrabbit.mk.model.AbstractNodeState;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.NodeStateEditor;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.mk.util.PathUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This {@code NodeStateEditor} implementation accumulates all changes into a json diff
 * and applies them to the microkernel on
 * {@link org.apache.jackrabbit.mk.model.NodeStore#merge(NodeStateEditor, NodeState)}.
 *
 * TODO: review/rewrite when OAK-45 is resolved
 * When the MicroKernel has support for branching and merging private working copies,
 * this implementation could:
 * - directly write every operation through to the private working copy
 * - batch write operations through to the private working copy when the
 *   transient space gets too big.
 * - spool write operations through to the private working copy on a background thread
 */
public class KernelNodeStateEditor implements NodeStateEditor {
    private final NodeState base;
    private final TransientNodeState transientState;
    private final StringBuilder jsop;

    KernelNodeStateEditor(NodeState base) {
        this.base = base;
        transientState = new TransientNodeState(base, this, null, "");
        jsop = new StringBuilder();
    }

    private KernelNodeStateEditor(KernelNodeStateEditor parentEditor,
            NodeState state, String name) {

        base = parentEditor.base;
        transientState = new TransientNodeState(state, this, parentEditor.getNodeState(), name);
        jsop = parentEditor.jsop;
    }

    private KernelNodeStateEditor(KernelNodeStateEditor parentEditor,
            TransientNodeState state) {
        
        base = parentEditor.base;
        transientState = state;
        jsop = parentEditor.jsop;
    }

    @Override
    public void addNode(String name) {
        if (!hasNode(transientState, name)) {
            transientState.addNode(name);
            jsop.append("+\"").append(path(name)).append("\":{}");
        }
    }

    @Override
    public void removeNode(String name) {
        if (hasNode(transientState, name)) {
            transientState.removeNode(name);
            jsop.append("-\"").append(path(name)).append('"');
        }
    }

    @Override
    public void setProperty(PropertyState state) {
        transientState.setProperty(state);
        jsop.append("^\"").append(path(state.getName())).append("\":")
                .append(state.getEncodedValue());
    }

    @Override
    public void removeProperty(String name) {
        transientState.removeProperty(name);
        jsop.append("^\"").append(path(name)).append("\":null");
    }

    @Override
    public void move(String sourcePath, String destPath) {
        TransientNodeState sourceParent = getTransientState(PathUtils.getAncestorPath(sourcePath, 1));
        String sourceName = PathUtils.getName(sourcePath);
        if (sourceParent == null || !hasNode(sourceParent, sourceName)) {
            return;
        }
        
        TransientNodeState destParent = getTransientState(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
        if (destParent == null || hasNode(destParent, destName)) {
            return;
        }

        sourceParent.move(sourceName, destParent, destName);
        jsop.append(">\"").append(path(sourcePath))
                .append("\":\"").append(path(destPath)).append('"');
    }

    @Override
    public void copy(String sourcePath, String destPath) {
        TransientNodeState sourceParent = getTransientState(PathUtils.getAncestorPath(sourcePath, 1));
        String sourceName = PathUtils.getName(sourcePath);
        if (sourceParent == null || !hasNode(sourceParent, sourceName)) {
            return;
        }

        TransientNodeState destParent = getTransientState(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
        if (destParent == null || hasNode(destParent, destName)) {
            return;
        }

        sourceParent.copy(sourceName, destParent, destName);
        jsop.append("*\"").append(path(sourcePath)).append("\":\"")
                .append(path(destPath)).append('"');
    }

    @Override
    public KernelNodeStateEditor edit(String name) {
        NodeState childState = transientState.getChildNode(name);
        if (childState == null) {
            return null;
        }
        else if (childState instanceof TransientNodeState) {
            return ((TransientNodeState) childState).editor;
        }
        else {
            return new KernelNodeStateEditor(this, childState, name);
        }
    }

    @Override
    public TransientNodeState getNodeState() {
        return transientState;
    }

    @Override
    public NodeState getBaseNodeState() {
        return base;
    }

    //------------------------------------------------------------< internal >---

    NodeState mergeInto(MicroKernel microkernel, KernelNodeState target) {
        String targetPath = target.getRevision();
        String targetRevision = target.getPath();
        String rev = microkernel.commit(targetPath, jsop.toString(), targetRevision, null);
        return new KernelNodeState(microkernel, targetPath, rev);
    }

    private TransientNodeState getTransientState(String path) {
        KernelNodeStateEditor editor = this;
        for(String name : PathUtils.elements(path)) {
            editor = editor.edit(name);
            if (editor == null) {
                return null;
            }
        }
        
        return editor.transientState;
    }

    private String path(String name) {
        String path = transientState.getPath();
        return path.isEmpty() ? name : path + '/' + name;
    }

    private static boolean hasNode(NodeState nodeState, String name) {
        return nodeState.getChildNode(name) != null;
    }

    private class TransientNodeState extends AbstractNodeState {
        private final KernelNodeStateEditor editor;
        private final NodeState persistentState;

        private final Map<String, NodeState> addedNodes = new HashMap<String, NodeState>();
        private final Set<String> removedNodes = new HashSet<String>();
        private final Map<String, PropertyState> addedProperties = new HashMap<String, PropertyState>();
        private final Set<String> removedProperties = new HashSet<String>();

        private String name;
        private TransientNodeState parent;

        public TransientNodeState(NodeState persistentState, KernelNodeStateEditor editor,
                TransientNodeState parent, String name) {

            this.editor = editor;
            this.persistentState = persistentState;
            this.parent = parent;
            this.name = name;
        }

        public TransientNodeState(KernelNodeStateEditor parentEditor, String name) {
            editor = new KernelNodeStateEditor(parentEditor, this);
            persistentState = null;
            parent = parentEditor.getNodeState();
            this.name = name;
        }

        @Override
        public PropertyState getProperty(String name) {
            PropertyState state = addedProperties.get(name);
            if (state != null) {
                return state;
            }

            return removedProperties.contains(name)
                ? null
                : persistentState.getProperty(name);
        }

        @Override
        public long getPropertyCount() {
            // todo optimise getPropertyCount
            // persistentCount - removedCount + addedCount won't work however since
            // persisted properties might be overlaid
            return super.getPropertyCount();
        }

        @Override
        public NodeState getChildNode(String name) {
            NodeState state = addedNodes.get(name);
            if (state != null) {
                return state;
            }

            return removedNodes.contains(name)
                ? null
                : persistentState.getChildNode(name);
        }

        @Override
        public long getChildNodeCount() {
            // todo optimise getChildNodeCount
            // persistentCount - removedCount + addedCount won't work however since
            // persisted nodes might be overlaid
            return super.getChildNodeCount();
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            final Set<String> removed = new HashSet<String>();
            removed.addAll(removedProperties);

            final Map<String, PropertyState> added = new HashMap<String, PropertyState>();
            added.putAll(addedProperties);

            final Iterable<? extends PropertyState>
                    persistedProperties = persistentState.getProperties();

            return new Iterable<PropertyState>() {
                @Override
                public Iterator<PropertyState> iterator() {
                    return new Iterator<PropertyState>() {
                        private final Iterator<? extends PropertyState>
                                properties = persistedProperties.iterator();
                        private PropertyState next;

                        @Override
                        public boolean hasNext() {
                            if (next == null) {
                                while (properties.hasNext()) {
                                    PropertyState prop = properties.next();
                                    if (added.containsKey(prop.getName())) {
                                        next = added.get(prop.getName());
                                        break;
                                    }
                                    if (!removed.contains(prop.getName())) {
                                        next = prop;
                                        break;
                                    }
                                }
                            }
                            return next != null;
                        }

                        @Override
                        public PropertyState next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            PropertyState e = next;
                            next = null;
                            return e;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }
                    };
                }
            };
        }

        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries(long offset, int count) {
            final Set<String> removed = new HashSet<String>();
            removed.addAll(removedNodes);

            final Map<String, NodeState> added = new HashMap<String, NodeState>();
            added.putAll(addedNodes);

            final Iterable<? extends ChildNodeEntry>
                    baseNodes = persistentState.getChildNodeEntries(offset, count);

            return new Iterable<ChildNodeEntry>() {
                @Override
                public Iterator<ChildNodeEntry> iterator() {
                    return new Iterator<ChildNodeEntry>() {
                        private final Iterator<? extends ChildNodeEntry>
                                properties = baseNodes.iterator();
                        private ChildNodeEntry next;

                        @Override
                        public boolean hasNext() {
                            if (next == null) {
                                while (properties.hasNext()) {
                                    final ChildNodeEntry entry = properties.next();
                                    if (added.containsKey(entry.getName())) {
                                        next = new AbstractChildNodeEntry() {
                                            @Override
                                            public String getName() {
                                                return entry.getName();
                                            }

                                            @Override
                                            public NodeState getNode() {
                                                return added.get(entry.getName());
                                            }
                                        };
                                        break;
                                    }
                                    if (!removed.contains(entry.getName())) {
                                        next = entry;
                                        break;
                                    }
                                }
                            }
                            return next != null;
                        }

                        @Override
                        public ChildNodeEntry next() {
                            if (!hasNext()) {
                                throw new NoSuchElementException();
                            }
                            ChildNodeEntry e = next;
                            next = null;
                            return e;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException("remove");
                        }
                    };
                }
            };
        }

        //------------------------------------------------------------< internal >---

        String getPath() {
            if (parent == null) {
                return name;
            }
            else {
                String path = parent.getPath();
                return path.isEmpty()
                    ? name
                    : path + '/' + name;
            }
        }

        void addNode(String name) {
            addedNodes.put(name, new TransientNodeState(editor, name));
        }

        void removeNode(String name) {
            addedNodes.remove(name);
            removedNodes.add(name);
        }

        void setProperty(PropertyState state) {
            addedProperties.put(state.getName(), state);
        }

        void removeProperty(String name) {
            addedProperties.remove(name);
            removedProperties.add(name);
        }

        void move(String name, TransientNodeState destParent, String destName) {
            NodeState state = getChildNode(name);
            removeNode(name);
            destParent.addNode(destName, state);
        }

        void copy(String name, TransientNodeState destParent, String destName) {
            NodeState state = getChildNode(name);
            destParent.addNode(destName, state);
        }

        private void addNode(String name, NodeState state) {
            addedNodes.put(name, state);
        }
    }
}
