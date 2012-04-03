package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.model.AbstractChildNodeEntry;
import org.apache.jackrabbit.mk.model.AbstractNodeState;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.PropertyState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class TransientNodeState extends AbstractNodeState {
    private final KernelNodeStateEditor editor;
    private final NodeState persistentState;

    private final Map<String, NodeState> addedNodes = new HashMap<String, NodeState>();
    private final Set<String> removedNodes = new HashSet<String>();
    private final Map<String, PropertyState> addedProperties = new HashMap<String, PropertyState>();
    private final Set<String> removedProperties = new HashSet<String>();

    private String name;
    private TransientNodeState parent;

    TransientNodeState(NodeState persistentState, KernelNodeStateEditor editor,
            TransientNodeState parent, String name) {

        this.editor = editor;
        this.persistentState = persistentState;
        this.parent = parent;
        this.name = name;
    }

    TransientNodeState(KernelNodeStateEditor parentEditor, String name) {
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

    KernelNodeStateEditor getEditor() {
        return editor;
    }

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
