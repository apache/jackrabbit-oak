/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.Predicate;

import java.util.Iterator;

/**
 * {@code NodeStore} implementations which supports batching changes
 * to the content tree up until a certain limit is reached and write them
 * down to the Microkernel in a single operation. The batch size is controlled
 * through {@link #PURGE_LIMIT} which is the number of characters on a commit
 * (i.e. jsop string).
 */
public class KernelNodeStore extends AbstractNodeStore {

    /**
     * Maximal size of size of a commit (number of characters of the corresponding
     * jsop string). When the limit is reached, changes kept in memory are written
     * back to the private branch in the Microkernel.
     */
    private static final int PURGE_LIMIT = 1024;  // TODO make configurable?

    /**
     * The {@link MicroKernel} instance used to store the content tree.
     */
    private final MicroKernel kernel;

    /**
     * Value factory backed by the {@link #kernel} instance.
     */
    private final CoreValueFactory valueFactory;

    /**
     * State of the current root node.
     */
    private KernelNodeState root;

    public KernelNodeStore(MicroKernel kernel) {
        this.kernel = kernel;
        this.valueFactory = new CoreValueFactoryImpl(kernel);
        this.root = new KernelNodeState(
                kernel, valueFactory, "/", kernel.getHeadRevision());
    }

    @Override
    public synchronized NodeState getRoot() {
        String revision = kernel.getHeadRevision();
        if (!revision.equals(root.getRevision())) {
            root = new KernelNodeState(
                    kernel, valueFactory, "/", kernel.getHeadRevision());
        }
        return root;
    }

    @Override
    public NodeStateBuilder getBuilder(NodeState base) {
        if (!(base instanceof KernelNodeState)) {
            throw new IllegalArgumentException("Alien node state");
        }

        KernelNodeState kernelNodeState = (KernelNodeState) base;
        String branchRevision = kernel.branch(kernelNodeState.getRevision());
        String path = kernelNodeState.getPath();
        KernelNodeState branchRoot = new KernelNodeState(kernel, valueFactory, path, branchRevision);
        return KernelNodeStateBuilder.create(new NodeStateBuilderContext(branchRoot));
    }

    @Override
    public void apply(NodeStateBuilder builder) throws CommitFailedException {
        if (!(builder instanceof KernelNodeStateBuilder)) {
            throw new IllegalArgumentException("Alien builder");
        }

        KernelNodeStateBuilder kernelNodeStateBuilder = (KernelNodeStateBuilder) builder;
        kernelNodeStateBuilder.getContext().applyPendingChanges();
    }

    @Override
    public CoreValueFactory getValueFactory() {
        return valueFactory;
    }

    //------------------------------------------------------------< internal >---

    /**
     * {@code NodeStateBuilderContext} keeps track of all changes to a
     * {@code KernelNodeStateBuilder} which have not yet been written back to the
     * Microkernel. It transforms the tree rooted at {@link #root} to reflect these
     * changes and writes these changes back to the Microkernel when
     * {@link KernelNodeStore#PURGE_LIMIT} is exceeded.
     */
    class NodeStateBuilderContext {

        /** Path of the root of the whole subtree */
        private final String path;

        /** Root of the subtree */
        private NodeState root;

        /** Current branch revision */
        private String revision;

        /** Pending changes */
        private StringBuilder jsop = new StringBuilder();

        NodeStateBuilderContext(KernelNodeState root) {
            this.path = root.getPath();
            this.root = root;
            this.revision = root.getRevision();
        }

        /**
         * @return path of the root of the whole subtree
         */
        String getPath() {
            return path;
        }

        /**
         * Get the node state located at {@code path}
         * @param path  path relative to {@link #root}
         * @return  node state at {@code path} or {@code null} if none.
         */
        NodeState getNodeState(String path) {
            NodeState state = root;
            for (String name : PathUtils.elements(path)) {
                state = state.getChildNode(name);
            }

            return state;
        }

        /**
         * Add a new, empty node state at {@code path}. The changes to the subtree
         * are reflected in {@link #root}.
         * @param relPath  path relative to {@link #root}. All but the last element
         *                 must resolve to existing node states.
         */
        void addNode(String relPath) {
            jsop.append("+\"").append(relPath).append("\":{}");
            root = addNode(root, MemoryNodeState.EMPTY_NODE, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        /**
         * Add a new node state at {@code path}. The changes to the subtree are reflected
         * in {@link #root}.
         * @param node     node state to add
         * @param relPath  path relative to {@link #root}. All but the last element
         *                 must resolve to existing node states.
         */
        void addNode(NodeState node, String relPath) {
            buildJsop(relPath, node);
            root = addNode(root, node, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        /**
         * Remove the node state at {@code path}. The changes to the subtree are reflected
         * in {@link #root}.
         * @param relPath  path relative to {@link #root}. All elements must resolve to
         *                 existing node states.
         */
        void removeNode(String relPath) {
            jsop.append("-\"").append(relPath).append('"');
            root = removeNode(root, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        /**
         * Add a new property state. The changes to the subtree are reflected in {@link #root}.
         * @param property     property state to add
         * @param parentPath   path to the parent node state relative to {@link #root}.
         *                     All elements must resolve to existing node states.
         */
        void addProperty(PropertyState property, String parentPath) {
            String path = PathUtils.concat(parentPath, property.getName());
            String value = property.isArray()
                    ? CoreValueMapper.toJsonArray(property.getValues())
                    : CoreValueMapper.toJsonValue(property.getValue());
            jsop.append("^\"").append(path).append("\":").append(value);
            root = addProperty(root, property, PathUtils.elements(parentPath).iterator());
            purgeOnLimit();
        }

        /**
         * Set an existing property state. The changes to the subtree are reflected in
         * {@link #root}.
         * @param property     property state to set
         * @param parentPath   path to the parent node state relative to {@link #root}.
         *                     All elements must resolve to existing node states.
         */
        void setProperty(PropertyState property, String parentPath) {
            String path = PathUtils.concat(parentPath, property.getName());
            String value = property.isArray()
                    ? CoreValueMapper.toJsonArray(property.getValues())
                    : CoreValueMapper.toJsonValue(property.getValue());
            jsop.append("^\"").append(path).append("\":").append(value);
            root = setProperty(root, property, PathUtils.elements(parentPath).iterator());
            purgeOnLimit();
        }

        /**
         * Remove an existing property state. The changes to the subtree are reflected in
         * {@link #root}.
         * @param relPath   path to the property state relative to {@link #root}. All
         *                  elements must resolve to existing node states.
         */
        void removeProperty(String relPath) {
            jsop.append("^\"").append(relPath).append("\":null");
            root = removeProperty(root, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        /**
         * Move the node from {@code sourcePath} to {@code destPath}. The changes to
         * the subtree are reflected in {@link #root}.
         * @param sourcePath  path to the node to move. All elements must resolve to
         *                    existing node states.
         * @param destPath    path to the new node. All but the last element must resolve
         *                    to existing node states.
         */
        void moveNode(String sourcePath, String destPath) {
            jsop.append(">\"").append(sourcePath).append("\":\"").append(destPath).append('"');
            NodeState moveNode = getChildNode(sourcePath);
            root = removeNode(root, PathUtils.elements(sourcePath).iterator());
            root = addNode(root, moveNode, PathUtils.elements(destPath).iterator());
            purgeOnLimit();
        }

        /**
         * Copy the node from {@code sourcePath} to {@code destPath}. The changes to
         * the subtree are reflected in {@link #root}.
         * @param sourcePath  path to the node to copy. All elements must resolve to
         *                    existing node states.
         * @param destPath    path to the new node. All but the last element must resolve
         *                    to existing node states.
         */
        void copyNode(String sourcePath, String destPath) {
            jsop.append("*\"").append(sourcePath).append("\":\"").append(destPath).append('"');
            NodeState copyNode = getChildNode(sourcePath);
            root = addNode(root, copyNode, PathUtils.elements(destPath).iterator());
            purgeOnLimit();
        }

        /**
         * Merge back into trunk
         * @throws CommitFailedException  if merging fails
         */
        void applyPendingChanges() throws CommitFailedException {
            try {
                purgePendingChanges();
                kernel.merge(revision, null);
                revision = null;
            }
            catch (MicroKernelException e) {
                throw new CommitFailedException(e);
            }
        }

        //------------------------------------------------------------< private >---

        /**
         * Purge all changes kept in memory to the private branch if
         * {@link KernelNodeStore#PURGE_LIMIT} is exceeded.
         * @see #purgePendingChanges()
         */
        private void purgeOnLimit() {
            if (jsop.length() > PURGE_LIMIT) {
                purgePendingChanges();
            }
        }

        /**
         * Purge all changes kept in memory to the private branch.
         */
        private void purgePendingChanges() {
            if (revision == null) {
                throw new IllegalStateException("Branch has been merged already");
            }

            if (jsop.length() > 0) {
                revision = kernel.commit(path, jsop.toString(), revision, null);
                root = new KernelNodeState(kernel, valueFactory, path, revision);
                jsop = new StringBuilder();
            }
        }

        /**
         * Build a jsop statement for adding a node state at a given path.
         * @param path        path where {@code nodeState} should be added.
         * @param nodeState   node state to add.
         */
        private void buildJsop(String path, NodeState nodeState) {
            jsop.append("+\"").append(path).append("\":{}");

            for (PropertyState property : nodeState.getProperties()) {
                String targetPath = PathUtils.concat(path, property.getName());
                String value = property.isArray()
                        ? CoreValueMapper.toJsonArray(property.getValues())
                        : CoreValueMapper.toJsonValue(property.getValue());

                jsop.append("^\"").append(targetPath).append("\":").append(value);
            }

            for (ChildNodeEntry child : nodeState.getChildNodeEntries()) {
                String targetPath = PathUtils.concat(path, child.getName());
                buildJsop(targetPath, child.getNodeState());
            }
        }

        /**
         * Construct a new {@code NodeState} where {@code node} is added to
         * {@code parent} at {@code path}.
         * @param parent  parent where {@code node} should be added
         * @param node    node state to add
         * @param path    path from {@code parent} where {@code node} should be added
         * @return  a new {@code NodeState} instance with the added node state.
         */
        private NodeState addNode(NodeState parent, NodeState node, Iterator<String> path) {
            String name = path.next();
            if (path.hasNext()) {
                return new SetNodeDecorator(parent, name, addNode(parent.getChildNode(name), node, path));
            }
            else {
                return new AddNodeDecorator(parent, name, node);
            }
        }

        /**
         * Construct a new {@code NodeState} where the node state at {@code path} is
         * removed from {@code parent}.
         * @param parent  parent from which the node state should be removed
         * @param path    path from {@code parent} for the node state to remove
         * @return  a new {@code NodeState} instance with the remove node state.
         */
        private NodeState removeNode(NodeState parent, Iterator<String> path) {
            String name = path.next();
            if (path.hasNext()) {
                return new SetNodeDecorator(parent, name, removeNode(parent.getChildNode(name), path));
            }
            else {
                return new RemoveNodeDecorator(parent, name);
            }
        }

        /**
         * Construct a new {@code NodeState} where {@code property} is added to
         * {@code parent} at {@code parentPath}.
         * @param parent      parent where {@code node} should be added
         * @param property    property state to add
         * @param parentPath  path from {@code parent} where {@code property} should be
         *                    added
         * @return  a new {@code NodeState} instance with the added property state.
         */
        private NodeState addProperty(NodeState parent, PropertyState property, Iterator<String> parentPath) {
            if (parentPath.hasNext()) {
                String name = parentPath.next();
                return new SetNodeDecorator(parent, name, addProperty(parent.getChildNode(name), property, parentPath));
            }
            else {
                return new AddPropertyDecorator(property, parent);
            }
        }

        /**
         * Construct a new {@code NodeState} where {@code property} is set to
         * {@code parent} at {@code parentPath}.
         * @param parent      parent where {@code node} should be set
         * @param property    property state to set
         * @param parentPath  path from {@code parent} where {@code property} should be
         *                    set
         * @return  a new {@code NodeState} instance with the new property state.
         */
        private NodeState setProperty(NodeState parent, PropertyState property, Iterator<String> parentPath) {
            if (parentPath.hasNext()) {
                String name = parentPath.next();
                return new SetNodeDecorator(parent, name, setProperty(parent.getChildNode(name), property, parentPath));
            }
            else {
                return new SetPropertyDecorator(property, parent);
            }
        }

        /**
         * Construct a new {@code NodeState} where the property state at {@code path} is
         * removed from {@code parent}.
         * @param parent  parent from which the property state should be removed
         * @param path    path from {@code parent} for the property state to remove
         * @return  a new {@code NodeState} instance with the remove property state.
         */
        private NodeState removeProperty(NodeState parent, Iterator<String> path) {
            String name = path.next();
            if (path.hasNext()) {
                return new SetNodeDecorator(parent, name, removeProperty(parent.getChildNode(name), path));
            }
            else {
                return new RemovePropertyDecorator(name, parent);
            }
        }

        /**
         * Get the node state located at {@code relPath} from {@link #root}.
         * @param relPath  relative path
         * @return  child node at {@code relPath} or {@code null} if none.
         */
        private NodeState getChildNode(String relPath) {
            NodeState state = root;
            for (String name : PathUtils.elements(relPath)) {
                state = state.getChildNode(name);
            }
            return state;
        }

        /**
         * {@code NodeState} decorator adding a new node state.
         */
        private class AddNodeDecorator extends AbstractNodeState {
            private final NodeState parent;
            private final String childName;
            private final NodeState node;

            /**
             * Construct a new {@code NodeState} from {@code parent} with {@code node} added
             * as new child with name {@code childName}.
             * @param parent
             * @param childName
             * @param node
             * @return
             */
            public AddNodeDecorator(NodeState parent, String childName, NodeState node) {
                this.parent = parent;
                this.childName = childName;
                this.node = node;
            }

            @Override
            public PropertyState getProperty(String name) {
                return parent.getProperty(name);
            }

            @Override
            public long getPropertyCount() {
                return parent.getPropertyCount();
            }

            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return parent.getProperties();
            }

            @Override
            public NodeState getChildNode(String name) {
                return childName.equals(name) ? node : parent.getChildNode(name);
            }

            @Override
            public long getChildNodeCount() {
                return 1 + parent.getChildNodeCount();
            }

            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                return new Iterable<ChildNodeEntry>() {
                    @Override
                    public Iterator<ChildNodeEntry> iterator() {
                        return Iterators.chain(
                            parent.getChildNodeEntries().iterator(),
                            Iterators.singleton(new MemoryChildNodeEntry(childName, node)));
                    }
                };
            }

        }

        /**
         * {@code NodeState} decorator modifying an existing node state to a new node state.
         */
        private class SetNodeDecorator extends AbstractNodeState {
            private final NodeState parent;
            private final String childName;
            private final NodeState node;

            /**
             * Construct a new {@code NodeState} from {@code parent} with child node state
             * {@code childName} replaced with {@code node}.
             * @param parent
             * @param childName
             * @param node
             * @return
             */
            public SetNodeDecorator(NodeState parent, String childName, NodeState node) {
                this.parent = parent;
                this.childName = childName;
                this.node = node;
            }

            @Override
            public PropertyState getProperty(String name) {
                return parent.getProperty(name);
            }

            @Override
            public long getPropertyCount() {
                return parent.getPropertyCount();
            }

            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return parent.getProperties();
            }

            @Override
            public NodeState getChildNode(String name) {
                return childName.equals(name) ? node : parent.getChildNode(name);
            }

            @Override
            public long getChildNodeCount() {
                return parent.getChildNodeCount();
            }

            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                return new Iterable<ChildNodeEntry>() {
                    @Override
                    public Iterator<ChildNodeEntry> iterator() {
                        return Iterators.map(parent.getChildNodeEntries().iterator(),
                            new Function1<ChildNodeEntry, ChildNodeEntry>() {
                                @Override
                                public ChildNodeEntry apply(ChildNodeEntry cne) {
                                    return childName.equals(cne.getName())
                                            ? new MemoryChildNodeEntry(childName, node)
                                            : cne;
                                }
                            });
                    }
                };
            }
        }

        /**
         * {@code NodeState} decorator removing a node state
         */
        private class RemoveNodeDecorator extends AbstractNodeState {
            private final NodeState parent;
            private final String childName;

            /**
             * Construct a new {@code NodeState} from {@code parent} with child node state
             * {@code childName} removed.
             * @param parent
             * @param childName
             * @return
             */
            public RemoveNodeDecorator(NodeState parent, String childName) {
                this.parent = parent;
                this.childName = childName;
            }

            @Override
            public PropertyState getProperty(String name) {
                return parent.getProperty(name);
            }

            @Override
            public long getPropertyCount() {
                return parent.getPropertyCount();
            }

            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return parent.getProperties();
            }

            @Override
            public NodeState getChildNode(String name) {
                return childName.equals(name) ? null : parent.getChildNode(name);
            }

            @Override
            public long getChildNodeCount() {
                return parent.getChildNodeCount() - 1;
            }

            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                return new Iterable<ChildNodeEntry>() {
                    @Override
                    public Iterator<ChildNodeEntry> iterator() {
                        return Iterators.filter(parent.getChildNodeEntries().iterator(),
                            new Predicate<ChildNodeEntry>() {
                                @Override
                                public boolean evaluate(ChildNodeEntry cne) {
                                    return !childName.equals(cne.getName());
                                }
                            }
                        );
                    }
                };
            }
        }

        /**
         * {@code NodeState} decorator adding a new property state
         */
        private class AddPropertyDecorator extends AbstractNodeState {
            private final PropertyState property;
            private final NodeState parent;

            /**
             * Construct a new {@code NodeState} from {@code parent} with {@code property}
             * added.
             * @param parent
             * @param property
             * @return
             */
            public AddPropertyDecorator(PropertyState property, NodeState parent) {
                this.property = property;
                this.parent = parent;
            }

            @Override
            public PropertyState getProperty(String name) {
                return property.getName().equals(name)
                    ? property
                    : parent.getProperty(name);
            }

            @Override
            public long getPropertyCount() {
                return parent.getPropertyCount() + 1;
            }

            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return new Iterable<PropertyState>() {
                    @Override
                    public Iterator<PropertyState> iterator() {
                        return Iterators.chain(
                                parent.getProperties().iterator(),
                                Iterators.singleton(property));
                    }
                };
            }

            @Override
            public NodeState getChildNode(String name) {
                return parent.getChildNode(name);
            }

            @Override
            public long getChildNodeCount() {
                return parent.getChildNodeCount();
            }

            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                return parent.getChildNodeEntries();
            }
        }

        /**
         * {@code NodeState} decorator modifying an existing property state.
         */
        private class SetPropertyDecorator extends AbstractNodeState {
            private final PropertyState property;
            private final NodeState parent;

            /**
             * Construct a new {@code NodeState} from {@code parent} with {@code property}
             * replaced.
             * @param parent
             * @param property
             * @return
             */
            public SetPropertyDecorator(PropertyState property, NodeState parent) {
                this.property = property;
                this.parent = parent;
            }

            @Override
            public PropertyState getProperty(String name) {
                return property.getName().equals(name)
                        ? property
                        : parent.getProperty(name);
            }

            @Override
            public long getPropertyCount() {
                return parent.getPropertyCount();
            }

            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return new Iterable<PropertyState>() {
                    @Override
                    public Iterator<PropertyState> iterator() {
                        return Iterators.map(parent.getProperties().iterator(),
                            new Function1<PropertyState, PropertyState>() {
                                @Override
                                public PropertyState apply(PropertyState state) {
                                    return property.getName().equals(state.getName())
                                            ? property
                                            : state;
                                }
                            }
                        );
                    }
                };
            }

            @Override
            public NodeState getChildNode(String name) {
                return parent.getChildNode(name);
            }

            @Override
            public long getChildNodeCount() {
                return parent.getChildNodeCount();
            }

            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                return parent.getChildNodeEntries();
            }
        }

        /**
         * {@code NodeState} decorator removing an existing property state.
         */
        private class RemovePropertyDecorator extends AbstractNodeState {
            private final String propertyName;
            private final NodeState parent;

            /**
             * Construct a new {@code NodeState} from {@code parent} with {@code propertyName}
             * removed.
             * @param parent
             * @param propertyName
             * @return
             */
            public RemovePropertyDecorator(String propertyName, NodeState parent) {
                this.propertyName = propertyName;
                this.parent = parent;
            }

            @Override
            public PropertyState getProperty(String name) {
                return propertyName.equals(name)
                    ? null
                    : parent.getProperty(name);
            }

            @Override
            public long getPropertyCount() {
                return parent.getPropertyCount() - 1;
            }

            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return new Iterable<PropertyState>() {
                    @Override
                    public Iterator<PropertyState> iterator() {
                        return Iterators.filter(parent.getProperties().iterator(),
                            new Predicate<PropertyState>() {
                                @Override
                                public boolean evaluate(PropertyState prop) {
                                    return !propertyName.equals(prop.getName());
                                }
                            }
                        );
                    }
                };
            }

            @Override
            public NodeState getChildNode(String name) {
                return parent.getChildNode(name);
            }

            @Override
            public long getChildNodeCount() {
                return parent.getChildNodeCount();
            }

            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                return parent.getChildNodeEntries();
            }
        }

    }

}
