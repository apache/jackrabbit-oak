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

package org.apache.jackrabbit.oak.jcr.state;

import org.apache.commons.collections.map.AbstractReferenceMap;
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.jackrabbit.ScalarImpl;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.oak.api.Scalar;
import org.apache.jackrabbit.oak.jcr.util.Iterators;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.apache.jackrabbit.oak.jcr.util.Predicate;
import org.apache.jackrabbit.oak.kernel.KernelPropertyState;

import javax.jcr.ItemExistsException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.PathNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.jcr.util.Unchecked.cast;

/**
 * A change tree records changes to a tree of nodes and properties. <p/>
 *
 * Internally a change tree is a tree of node deltas. A node delta describes whether
 * a node has been added, removed, moved or whether its properties have been changed.
 * A change tree contains a node delta for each touched node. A node is touched if it
 * is modified or one of its child nodes is touched. A node is modified if it is
 * transient or has modified properties. A node is transient if it is either added,
 * removed or moved. <p/>
 *
 * A move operation is conceptually handled as a remove operation followed by an add
 * operation of the respective sub tree. <p/>
 */
public class ChangeTree {
    private final NodeDelta root;
    private final Predicate<Path> nodeExists;
    private final Listener listener;

    /** Keep Existing instances at least as long as referenced by a client */
    private final Map<Path, Existing> existing = cast(new ReferenceMap(AbstractReferenceMap.HARD, AbstractReferenceMap.WEAK));

    /**
     * Listener for modifications in the hierarchy
     */
    public interface Listener {
        void added(NodeDelta nodeDelta);
        void removed(NodeDelta nodeDelta);
        void moved(Path source, NodeDelta nodeDelta);
        void setProperty(NodeDelta parent, PropertyState state);
        void removeProperty(NodeDelta parent, String name);
    }

    /**
     * Create a new change tree rooted at {@code rootPath}.
     * @param rootPath  root path for this change tree
     * @param listener  listener for changes in the hierarchy
     * @param nodeExists  predicate which determines whether a path exists on the
     *                    persistent layer.
     */
    public ChangeTree(final Path rootPath, Listener listener, Predicate<Path> nodeExists) {
        this.nodeExists = nodeExists;
        this.listener = listener;
        
        root = new Existing(null, "", rootPath) {
            @Override
            public Path getPath() {
                return rootPath;
            }
        };
    }

    /**
     * @return {@code true} iff {@code path} exists either transiently or on
     * the persistence layer.
     */
    public boolean nodeExists(Path path) {
        return getNode(path) != null;
    }

    /**
     * @param path
     * @return a {@code NodeDelta} instance for the given {@code path} or {@code null}
     * if {@code path} does not exist transiently nor on the persistence layer.
     */
    public NodeDelta getNode(Path path) {
        NodeDelta delta = root;
        for (String name : path.getNames()) {
            delta = delta.getNode(name);
            if (delta == null) {
                return null;
            }
        }
        return delta;
    }

    /**
     * @return  {@code true} iff this change tree has transient changes.
     */
    public boolean hasChanges() {  
        return root.hasChanges();
    }

    /**
     * {@code NodeDelta} instances record changes to a node. {@code NodeDelta}'s
     * subclasses correspond to these changes:
     *
     * <ul>
     * <li>{@link org.apache.jackrabbit.oak.jcr.state.ChangeTree.Added} represents a transiently
     *      added node.</li>
     * <li>{@link org.apache.jackrabbit.oak.jcr.state.ChangeTree.Removed} represents a transiently
     *      removed node.</li>
     * <li>{@link org.apache.jackrabbit.oak.jcr.state.ChangeTree.Existing} represents a node which
     *      is otherwise touched. That is, which either has property modifications or a has a
     *      child node which is touched. </li>
     * </ul>
     */
    public abstract class NodeDelta {
        private final Map<String, NodeDelta> childNodes = new HashMap<String, NodeDelta>();
        private final Map<String, PropertyState> properties = new HashMap<String, PropertyState>();
        protected NodeDelta parent;
        protected String name;

        NodeDelta(NodeDelta parent, String name) {
            this.parent = parent;
            this.name = name;
        }

        /**
         * @return the parent of this node
         */
        public NodeDelta getParent() {
            return parent;
        }

        /**
         * @return transient path to this node
         */
        public Path getPath() {
            return parent.getPath().concat(name);
        }

        /**
         * @return transient name of this node
         */
        public String getName() {
            return name;
        }

        /**
         * @return persistent path to this node or {@code null} if this node is not
         * an {@link org.apache.jackrabbit.oak.jcr.state.ChangeTree.Existing existing} node.
         */
        public Path getPersistentPath() {
            return null;
        }

        /**
         * @return {@code true} iff this node has been transiently removed.
         */
        public abstract boolean isRemoved();

        /**
         * @return {@code true} iff this node has been transiently added.
         */
        public abstract boolean isAdded();

        /**
         * @return {@code true} iff this node has been transiently moved.
         */
        public abstract boolean isMoved();

        /**
         * @return {@code true} iff this node is transient.
         */
        public abstract boolean isTransient();

        /**
         * @return {@code true} iff this node has changes. A node has changes
         * iff it either has changed properties or one of its child nodes has changes.
         */
        public boolean hasChanges() {
            return !properties.isEmpty() || !childNodes.isEmpty();
        }

        /**
         * @param name
         * @return  {@code true} iff this node has a child node with the given {@code name}.
         */
        public final boolean hasNode(String name) {
            return getNode(name) != null;
        }

        /**
         * @param name
         * @return  the child node with the given {@code name} or {@code null} if none.
         */
        public abstract NodeDelta getNode(String name);

        /**
         * @return  Iterator of all added nodes
         */
        public Iterator<NodeDelta> getNodes() {
            return Iterators.filter(childNodes().iterator(), new Predicate<NodeDelta>() {
                @Override
                public boolean evaluate(NodeDelta delta) {
                    return delta.isTransient() && !delta.isRemoved();
                }
            });
        }

        /**
         * @param name
         * @return  {@code true} iff this node has a modified child node of the given {@code name}.
         */
        public boolean isNodeModified(String name) {
            NodeDelta node = childNodes.get(name);
            return node != null && node.isTransient();
        }

        /**
         * @param name
         * @return {@code true} iff a property with the given name has been added,
         * removed or modified.
         */
        public boolean hasProperty(String name) {
            return properties.containsKey(name);
        }

        /**
         * @param name
         * @return  the state of the property with the given {@code name} or
         * {@code null} if if does not exist.
         */
        public PropertyState getPropertyState(String name) {
            return properties.get(name);
        }

        /**
         * @return  an iterator for all added and modified property states.
         */
        public Iterator<PropertyState> getPropertyStates() {
            return Iterators.filter(properties.values().iterator(),
                    new Predicate<PropertyState>() {
                        @Override
                        public boolean evaluate(PropertyState state) {
                            return !((KernelPropertyState) state).getValue().equals(ScalarImpl.nullScalar());  // fixme don't cast
                        }
                    });
        }

        /**
         * Add a node with the given {@code name}.
         * @param name
         * @return  the added node
         * @throws javax.jcr.ItemExistsException
         */
        public NodeDelta addNode(String name) throws ItemExistsException {
            if (hasNode(name)) {
                throw new ItemExistsException(name);
            }

            NodeDelta added = addChild(new Added(this, name));
            notifyAdded(added);
            return added;
        }

        /**
         * Remove the node with the given {@code name}.
         * @param name
         * @return  the removed node
         * @throws javax.jcr.ItemNotFoundException
         */
        public NodeDelta removeNode(String name) throws ItemNotFoundException {
            NodeDelta delta = getNode(name);
            if (delta == null) {
                throw new ItemNotFoundException(name);
            }

            NodeDelta removed = delta.remove();
            notifyRemoved(removed);
            return removed;
        }

        /**
         * Move the node with the given {@code name} to {@code destination}.
         * @param name
         * @param destination
         * @throws javax.jcr.ItemNotFoundException
         * @throws javax.jcr.ItemExistsException
         * @throws javax.jcr.PathNotFoundException
         */
        public void moveNode(String name, Path destination) throws ItemNotFoundException, ItemExistsException,
                PathNotFoundException {

            NodeDelta source = getNode(name);
            if (source == null) {
                throw new ItemNotFoundException(name);
            }

            if (nodeExists(destination)) {
                throw new ItemExistsException(destination.toJcrPath());
            }

            Path destParentPath = destination.getParent();
            if (!nodeExists(destParentPath)) {
                throw new PathNotFoundException(destParentPath.toJcrPath());
            }

            Path sourcePath = source.getPath();
            NodeDelta moved = source.moveTo(destParentPath, destination.getName());
            notifyMoved(sourcePath, moved);
        }

        /**
         * Set the property with the given {@code name} to {@code value} or remove the
         * property if {@code value} is {@code null}.
         * @param name
         * @param value
         */
        public void setValue(String name, Scalar value) {
            if (value == null) {
                value = ScalarImpl.nullScalar();
            }

            if (value.equals(ScalarImpl.nullScalar()) && properties.containsKey(name) &&
                    !properties.get(name).equals(ScalarImpl.nullScalar())) {

                properties.remove(name);
                notifyRemoveProperty(this, name);
            }
            else {
                KernelPropertyState state = new KernelPropertyState(name, value);
                properties.put(name, state);
                touch();
                notifySetProperty(this, state);
            }
        }

        /**
         * Set the property with the given {@code name} to {@code values}.
         * @param name
         * @param values
         */
        public void setValue(String name, List<Scalar> values) {
            KernelPropertyState state = new KernelPropertyState(name, values);
            properties.put(name, state);
            touch();
            notifySetProperty(this, state);
        }

        //------------------------------------------< internal >---

        void touch() { }

        NodeDelta remove() {
            return parent.addChild(new Removed(parent, name));
        }

        NodeDelta moveTo(Path parentPath, String name) {
            remove();
            this.name = name;
            NodeDelta parent = ChangeTree.this.getNode(parentPath);
            return parent.addChild(this);
        }

        final void clear() {
            childNodes.clear();
            properties.clear();
        }

        final Iterable<NodeDelta> childNodes() {
            return childNodes.values();
        }

        final NodeDelta getChild(String name) {
            return childNodes.get(name);
        }

        final boolean hasChild(String name) {
            return childNodes.containsKey(name);
        }

        final NodeDelta addChild(NodeDelta delta) {
            childNodes.put(delta.name, delta);
            delta.parent = this;
            touch();
            return delta;
        }

        private void notifyAdded(NodeDelta added) {
            if (listener != null) {
                listener.added(added);
            }
        }
        
        private void notifyRemoved(NodeDelta removed) {
            if (listener != null) {
                listener.removed(removed);
            }
        }
        
        private void notifyMoved(Path sourcePath, NodeDelta moved) {
            if (listener != null) {
                listener.moved(sourcePath, moved);
            }
        }
        
        private void notifySetProperty(NodeDelta parent, PropertyState state) {
            if (listener != null) {
                listener.setProperty(parent, state);
            }
        }

        private void notifyRemoveProperty(NodeDelta parent, String name) {
            if (listener != null) {
                listener.removeProperty(parent, name);
            }
        }
    }

    //------------------------------------------< private/internal >---

    /**
     * @return A {@code Existing} instance for the given {@code parent} and {@code name}.
     * Returns a previously allocated instance if not yet garbage collected.
     * <em>Note:</em> returning fresh instances while previously allocated ones are still
     * referenced in client code results in schizophrenia: same node multiple states.
     */
    private Existing existing(NodeDelta parent, String name, Path persistentPath) {
        Existing e = existing.get(persistentPath);
        if (e == null) {
            e = new Existing(parent, name, persistentPath);
            existing.put(persistentPath, e);
        }
        return e;
    }

    /**
     * Represents an existing node. That is, a node which exists on the persistence layer.
     */
    private class Existing extends NodeDelta {
        private final Path persistentPath;
        private boolean isMoved;

        Existing(NodeDelta parent, String name, Path persistentPath) {
            super(parent, name);
            this.persistentPath = persistentPath;
        }

        @Override
        public Path getPersistentPath() {
            return persistentPath;
        }

        @Override
        public boolean isRemoved() {
            return false;
        }

        @Override
        public boolean isAdded() {
            return false;
        }

        @Override
        public boolean isMoved() {
            return isMoved;
        }

        @Override
        public boolean isTransient() {
            return !isMoved;
        }

        @Override
        public NodeDelta getNode(String name) {
            NodeDelta delta = getChild(name);
            if (delta == null) {
                Path path = persistentPath.concat(name);
                return nodeExists.evaluate(path)
                        ? existing(this, name, path)
                        : null;
            }
            else {
                return delta.isRemoved() ? null : delta;
            }
        }

        @Override
        void touch() {
            if (parent != null && ! parent.hasChild(name)) {
                parent.addChild(this);
            }
        }

        @Override
        public String toString() {
            return "Existing[" + getPath() + ']';
        }

        //------------------------------------------< internal >---

        @Override
        NodeDelta moveTo(Path parentPath, String name) {
            isMoved = true;
            return super.moveTo(parentPath, name);
        }
    }

    /**
     * Represents a transiently added node.
     */
    private class Added extends NodeDelta {
        Added(NodeDelta parent, String name) {
            super(parent, name);
        }

        @Override
        public boolean isRemoved() {
            return false;
        }

        @Override
        public boolean isAdded() {
            return true;
        }

        @Override
        public boolean isMoved() {
            return false;
        }

        @Override
        public boolean isTransient() {
            return true;
        }

        @Override
        public NodeDelta getNode(String name) {
            NodeDelta delta = getChild(name);
            return delta == null || delta.isRemoved() ? null : delta;
        }

        @Override
        public String toString() {
            return "Added[" + getPath() + ']';
        }
    }

    /**
     * Represents a transiently removed node.
     */
    private class Removed extends NodeDelta {
        Removed(NodeDelta parent, String name) {
            super(parent, name);
        }

        @Override
        public boolean isRemoved() {
            return true;
        }

        @Override
        public boolean isAdded() {
            return false;
        }

        @Override
        public boolean isMoved() {
            return false;
        }

        @Override
        public boolean isTransient() {
            return true;
        }

        @Override
        public NodeDelta getNode(String name) {
            throw new IllegalStateException("Removed");
        }

        @Override
        public NodeDelta addNode(String name) {
            throw new IllegalStateException("Removed");
        }

        @Override
        public NodeDelta removeNode(String name) {
            throw new IllegalStateException("Removed");
        }

        @Override
        public void moveNode(String name, Path destination) {
            throw new IllegalStateException("Removed");
        }

        @Override
        public void setValue(String name, Scalar value) {
            throw new IllegalStateException("Removed");
        }

        @Override
        NodeDelta remove() {
            throw new IllegalStateException("Removed");
        }

        @Override
        NodeDelta moveTo(Path parentPath, String name) {
            throw new IllegalStateException("Removed");
        }

        @Override
        public String toString() {
            return "Removed[" + getPath() + ']';
        }
    }

}
