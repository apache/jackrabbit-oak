package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.Predicate;

import java.util.Iterator;

public class KernelNodeStore2 extends AbstractNodeStore {
    /**
     * The {@link org.apache.jackrabbit.mk.api.MicroKernel} instance used to store the content tree.
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

    public KernelNodeStore2(MicroKernel kernel) {
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
        return KernelNodeStateBuilder2.create(new NodeStateBuilderContext(branchRoot));
    }

    @Override
    public void apply(NodeStateBuilder builder) throws CommitFailedException {
        if (!(builder instanceof  KernelNodeStateBuilder2)) {
            throw new IllegalArgumentException("Alien builder");
        }

        KernelNodeStateBuilder2 kernelNodeStateBuilder = (KernelNodeStateBuilder2) builder;
        kernelNodeStateBuilder.getContext().applyPendingChanges();
    }

    @Override
    public CoreValueFactory getValueFactory() {
        return valueFactory;
    }

    //------------------------------------------------------------< internal >---

    class NodeStateBuilderContext {
        private static final int PURGE_LIMIT = 1024;  // TODO make configurable?

        private final String path;

        private NodeState root;
        private String revision;

        private StringBuilder jsop = new StringBuilder();

        NodeStateBuilderContext(KernelNodeState root) {
            this.path = root.getPath();
            this.root = root;
            this.revision = root.getRevision();
        }

        String getPath() {
            return path;
        }

        NodeState getNodeState(String path) {
            NodeState state = root;
            for (String name : PathUtils.elements(path)) {
                state = state.getChildNode(name);
            }

            return state;
        }

        void addNode(String relPath) {
            jsop.append("+\"").append(relPath).append("\":{}");
            root = addNode(root, EMPTY_STATE, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        void addNode(NodeState node, String relPath) {
            buildJsop(relPath, node);
            root = addNode(root, node, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        void removeNode(String relPath) {
            jsop.append("-\"").append(relPath).append('"');
            root = removeNode(root, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        void addProperty(PropertyState property, String parentPath) {
            String path = PathUtils.concat(parentPath, property.getName());
            String value = property.isArray()
                    ? CoreValueMapper.toJsonArray(property.getValues())
                    : CoreValueMapper.toJsonValue(property.getValue());
            jsop.append("^\"").append(path).append("\":").append(value);
            root = addProperty(root, property, PathUtils.elements(parentPath).iterator());
            purgeOnLimit();
        }

        void setProperty(PropertyState property, String parentPath) {
            String path = PathUtils.concat(parentPath, property.getName());
            String value = property.isArray()
                    ? CoreValueMapper.toJsonArray(property.getValues())
                    : CoreValueMapper.toJsonValue(property.getValue());
            jsop.append("^\"").append(path).append("\":").append(value);
            root = setProperty(root, property, PathUtils.elements(parentPath).iterator());
            purgeOnLimit();
        }

        void removeProperty(String relPath) {
            jsop.append("^\"").append(relPath).append("\":null");
            root = removeProperty(root, PathUtils.elements(relPath).iterator());
            purgeOnLimit();
        }

        void moveNode(String sourcePath, String destPath) {
            jsop.append(">\"").append(sourcePath).append("\":\"").append(destPath).append('"');
            NodeState moveNode = getChildNode(root, sourcePath);
            root = removeNode(root, PathUtils.elements(sourcePath).iterator());
            root = addNode(root, moveNode, PathUtils.elements(destPath).iterator());
            purgeOnLimit();
        }

        void copyNode(String sourcePath, String destPath) {
            jsop.append("*\"").append(sourcePath).append("\":\"").append(destPath).append('"');
            NodeState copyNode = getChildNode(root, sourcePath);
            root = addNode(root, copyNode, PathUtils.elements(destPath).iterator());
            purgeOnLimit();
        }

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

        private void purgeOnLimit() {
            if (jsop.length() > PURGE_LIMIT) {
                purgePendingChanges();
            }
        }

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

        private void buildJsop(String path, NodeState nodeState) {
            jsop.append("+\"").append(path).append("\":{}");

            for (PropertyState property : nodeState.getProperties()) {
                String targetPath = PathUtils.concat(path, property.getName());
                String value = property.isArray()
                        ? CoreValueMapper.toJsonArray(property.getValues())
                        : CoreValueMapper.toJsonValue(property.getValue());

                jsop.append("^\"").append(targetPath).append("\":").append(value);
            }

            for (ChildNodeEntry child : nodeState.getChildNodeEntries(0, -1)) {
                String targetPath = PathUtils.concat(path, child.getName());
                buildJsop(targetPath, child.getNodeState());
            }
        }

        private NodeState addNode(NodeState parent, NodeState node, Iterator<String> path) {
            String name = path.next();
            if (path.hasNext()) {
                return setChildNode(parent, name, addNode(parent.getChildNode(name), node, path));
            }
            else {
                return addChildNode(parent, name, node);
            }
        }

        private NodeState removeNode(NodeState parent, Iterator<String> path) {
            String name = path.next();
            if (path.hasNext()) {
                return setChildNode(parent, name, removeNode(parent.getChildNode(name), path));
            }
            else {
                return removeChildNode(parent, name);
            }
        }

        private NodeState addProperty(NodeState parent, PropertyState added, Iterator<String> parentPath) {
            if (parentPath.hasNext()) {
                String name = parentPath.next();
                return setChildNode(parent, name, addProperty(parent.getChildNode(name), added, parentPath));
            }
            else {
                return addChildProperty(parent, added);
            }
        }

        private NodeState setProperty(NodeState parent, PropertyState property, Iterator<String> parentPath) {
            if (parentPath.hasNext()) {
                String name = parentPath.next();
                return setChildNode(parent, name, setProperty(parent.getChildNode(name), property, parentPath));
            }
            else {
                return setChildProperty(parent, property);
            }
        }

        private NodeState removeProperty(NodeState parent, Iterator<String> path) {
            String name = path.next();
            if (path.hasNext()) {
                return setChildNode(parent, name, removeProperty(parent.getChildNode(name), path));
            }
            else {
                return removeChildProperty(parent, name);
            }
        }

        private NodeState getChildNode(NodeState state, String relPath) {
            for (String name : PathUtils.elements(relPath)) {
                state = state.getChildNode(name);
            }
            return state;
        }

        private final NodeState EMPTY_STATE = new AbstractNodeState() {
            @Override
            public PropertyState getProperty(String name) {
                return null;
            }

            @Override
            public long getPropertyCount() {
                return 0;
            }

            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return new Iterable<PropertyState>() {
                    @Override
                    public Iterator<PropertyState> iterator() {
                        return Iterators.empty();
                    }
                };
            }

            @Override
            public NodeState getChildNode(String name) {
                return null;
            }

            @Override
            public long getChildNodeCount() {
                return 0;
            }

            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries(long offset, int count) {
                return new Iterable<ChildNodeEntry>() {
                    @Override
                    public Iterator<ChildNodeEntry> iterator() {
                        return Iterators.empty();
                    }
                };
            }
        };

        private ChildNodeEntry createCNE(final String name, final NodeState state) {
            return new AbstractChildNodeEntry() {
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public NodeState getNodeState() {
                    return state;
                }
            };
        }

        private NodeState addChildNode(final NodeState parent, final String childName, final NodeState node) {
            return new AbstractNodeState() {
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
                public Iterable<? extends ChildNodeEntry> getChildNodeEntries(final long offset, final int count) {
                    if (offset >= getChildNodeCount()) {
                        return new Iterable<ChildNodeEntry>() {
                            @Override
                            public Iterator<ChildNodeEntry> iterator() {
                                return Iterators.empty();
                            }
                        };
                    }
                    else if (count == -1 || offset + count > getChildNodeCount()) {
                        return new Iterable<ChildNodeEntry>() {
                            @Override
                            public Iterator<ChildNodeEntry> iterator() {
                                return Iterators.chain(
                                    parent.getChildNodeEntries(offset, count).iterator(),
                                    Iterators.singleton(createCNE(childName, node)));
                            }
                        };
                    }
                    else {
                        return parent.getChildNodeEntries(offset, count);
                    }
                }

            };
        }

        private NodeState setChildNode(final NodeState parent, final String childName, final NodeState node) {
            return new AbstractNodeState() {
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
                public Iterable<? extends ChildNodeEntry> getChildNodeEntries(final long offset, final int count) {
                    return new Iterable<ChildNodeEntry>() {
                        @Override
                        public Iterator<ChildNodeEntry> iterator() {
                            return Iterators.map(parent.getChildNodeEntries(offset, count).iterator(),
                                new Function1<ChildNodeEntry, ChildNodeEntry>() {
                                    @Override
                                    public ChildNodeEntry apply(ChildNodeEntry cne) {
                                        return childName.equals(cne.getName())
                                                ? createCNE(childName, node)
                                                : cne;
                                    }
                                });
                        }
                    };
                }
            };
        }

        private NodeState removeChildNode(final NodeState parent, final String childName) {
            return new AbstractNodeState() {
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
                public Iterable<? extends ChildNodeEntry> getChildNodeEntries(final long offset, final int count) {
                    return new Iterable<ChildNodeEntry>() {
                        @Override
                        public Iterator<ChildNodeEntry> iterator() {
                            return Iterators.filter(parent.getChildNodeEntries(offset, count).iterator(), // FIXME offsetting doesn't compose with filtering
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
            };
        }

        private NodeState addChildProperty(final NodeState parent, final PropertyState property) {
            return new AbstractNodeState() {
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
                public Iterable<? extends ChildNodeEntry> getChildNodeEntries(long offset, int count) {
                    return parent.getChildNodeEntries(offset, count);
                }
            };
        }

        private NodeState setChildProperty(final NodeState parent, final PropertyState property) {
            return new AbstractNodeState() {
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
                public Iterable<? extends ChildNodeEntry> getChildNodeEntries(long offset, int count) {
                    return parent.getChildNodeEntries(offset, count);
                }
            };
        }

        private NodeState removeChildProperty(final NodeState parent, final String propertyName) {
            return new AbstractNodeState() {
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
                public Iterable<? extends ChildNodeEntry> getChildNodeEntries(long offset, int count) {
                    return parent.getChildNodeEntries(offset, count);
                }
            };
        }

    }
}
