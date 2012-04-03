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

public class KernelNodeStateEditor implements NodeStateEditor {
    private final NodeState base;
    private final String path;
    private final StringBuilder jsop;

    private final Map<String, NodeState> addedNodes = new HashMap<String, NodeState>();
    private final Set<String> removedNodes = new HashSet<String>();
    private final Map<String, PropertyState> addedProperties = new HashMap<String, PropertyState>();
    private final Set<String> removedProperties = new HashSet<String>();

    KernelNodeStateEditor(NodeState base) {
        this.base = base;
        this.path = "";
        this.jsop = new StringBuilder();
    }

    private KernelNodeStateEditor(NodeState base, String path, StringBuilder jsop) {
        this.base = base;
        this.path = path;
        this.jsop = jsop;
    }

    @Override
    public void addNode(String name) {
        if (!hasNodeState(name)) {
            transientAddNode(name, new TransientNodeState(name));
            jsop.append("+\"").append(path(name)).append("\":{}");
        }
    }

    @Override
    public void removeNode(String name) {
        if (hasNodeState(name)) {
            transientRemoveNode(name);
            jsop.append("-\"").append(path(name)).append('"');
        }
    }

    @Override
    public void setProperty(PropertyState state) {
        transientSetProperty(state);
        jsop.append("^\"").append(path(state.getName())).append("\":")
                .append(state.getEncodedValue());
    }

    @Override
    public void removeProperty(String name) {
        transientRemoveProperty(name);
        jsop.append("^\"").append(path(name)).append("\":null");
    }

    @Override
    public void move(String sourcePath, String destPath) {
        KernelNodeStateEditor sourceParent = getEditor(PathUtils.getAncestorPath(sourcePath, 1));
        String sourceName = PathUtils.getName(sourcePath);
        if (sourceParent == null || !sourceParent.hasNodeState(sourceName)) {
            return;
        }
        
        KernelNodeStateEditor destParent = getEditor(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
        if (destParent == null || destParent.hasNodeState(destName)) {
            return;
        }

        destParent.transientAddNode(destName, sourceParent.transientRemoveNode(sourceName));
        jsop.append(">\"").append(path(sourcePath))
                .append("\":\"").append(path(destPath)).append('"');
    }

    @Override
    public void copy(String sourcePath, String destPath) {
        KernelNodeStateEditor source = getEditor(sourcePath);
        if (source == null) {
            return;
        }

        KernelNodeStateEditor destParent = getEditor(PathUtils.getAncestorPath(destPath, 1));
        String destName = PathUtils.getName(destPath);
        if (destParent == null || destParent.hasNodeState(destName)) {
            return;
        }

        destParent.transientAddNode(destName, source.getNodeState());
        jsop.append("*\"").append(path(sourcePath)).append("\":\"")
                .append(path(destPath)).append('"');
    }

    @Override
    public KernelNodeStateEditor edit(String name) {
        NodeState childState = getChildNodeState(name);
        if (childState == null) {
            return null;
        }
        else if (childState instanceof TransientNodeState) {
            return ((TransientNodeState) childState).editor;
        }
        else {
            return new KernelNodeStateEditor(childState, cat(path, name), jsop);
        }
    }

    @Override
    public NodeState getNodeState() {
        return new TransientNodeState(this);
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

    private KernelNodeStateEditor getEditor(String path) {
        KernelNodeStateEditor editor = this;
        for(String name : PathUtils.elements(path)) {
            editor = editor.edit(name);
            if (editor == null) {
                return null;
            }
        }
        
        return editor;
    }

    private static String cat(String path, String relPath) {
        return path.isEmpty() ? relPath : path + '/' + relPath;
    }

    private String path(String name) {
        return cat(path, name);
    }

    private NodeState getChildNodeState(String name) {
        NodeState state = addedNodes.get(name);
        if (state != null) {
            return state;
        }

        return removedNodes.contains(name)
            ? null
            : base.getChildNode(name);
    }

    private boolean hasNodeState(String name) {
        return getChildNodeState(name) != null;
    }

    private PropertyState getPropertyState(String name) {
        PropertyState state = addedProperties.get(name);
        if (state != null) {
            return state;
        }

        if (removedProperties.contains(name)) {
            return null;
        }
        else {
            return base.getProperty(name);
        }
    }

    private void transientAddNode(String name, NodeState nodeState) {
        addedNodes.put(name, nodeState);
    }

    private NodeState transientRemoveNode(String name) {
        NodeState state = addedNodes.remove(name);
        removedNodes.add(name);
        return state == null
            ? base.getChildNode(name)
            : state;
    }

    private void transientSetProperty(PropertyState state) {
        addedProperties.put(state.getName(), state);
    }

    private void transientRemoveProperty(String name) {
        addedProperties.remove(name);
        removedProperties.add(name);
    }

    private class TransientNodeState extends AbstractNodeState {
        private final KernelNodeStateEditor editor;

        public TransientNodeState(KernelNodeStateEditor editor) {
            this.editor = editor;
        }

        public TransientNodeState(String name) {
            this.editor = new KernelNodeStateEditor(this, cat(path, name), jsop);
        }

        @Override
        public PropertyState getProperty(String name) {
            return editor.getPropertyState(name);
        }

        @Override
        public NodeState getChildNode(String name) {
            return editor.getChildNodeState(name);
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            final Set<String> removed = new HashSet<String>();
            removed.addAll(editor.removedProperties);

            final Map<String, PropertyState> added = new HashMap<String, PropertyState>();
            added.putAll(editor.addedProperties);

            final Iterable<? extends PropertyState> baseProperties = editor.base.getProperties();
            return new Iterable<PropertyState>() {
                @Override
                public Iterator<PropertyState> iterator() {
                    return new Iterator<PropertyState>() {
                        private final Iterator<? extends PropertyState> properties = baseProperties.iterator();
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
            removed.addAll(editor.removedNodes);

            final Map<String, NodeState> added = new HashMap<String, NodeState>();
            added.putAll(editor.addedNodes);

            final Iterable<? extends ChildNodeEntry> baseNodes = editor.base.getChildNodeEntries(offset, count);
            return new Iterable<ChildNodeEntry>() {
                @Override
                public Iterator<ChildNodeEntry> iterator() {
                    return new Iterator<ChildNodeEntry>() {
                        private final Iterator<? extends ChildNodeEntry> properties = baseNodes.iterator();
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
    }
}
