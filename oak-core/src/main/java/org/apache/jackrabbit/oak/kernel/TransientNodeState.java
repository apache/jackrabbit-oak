package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.PropertyState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

public class TransientNodeState {
    private final KernelNodeStateEditor editor;
    private final NodeState persistentState;

    private final Map<NodeState, TransientNodeState> existingChildNodes =
            new HashMap<NodeState, TransientNodeState>();
    private final Map<String, TransientNodeState> addedNodes =
            new HashMap<String, TransientNodeState>();
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

    private TransientNodeState(KernelNodeStateEditor parentEditor, String name) {
        this(parentEditor, name, null);
    }
    
    private TransientNodeState(KernelNodeStateEditor parentEditor, String name,
            NodeState persistedState) {

        editor = new KernelNodeStateEditor(parentEditor, this);
        this.persistentState = persistedState;
        parent = parentEditor.getTransientState();
        this.name = name;
    }

    private TransientNodeState(TransientNodeState state, TransientNodeState parent,
            String name) {

        editor = new KernelNodeStateEditor(parent.getEditor(), this);
        persistentState = state.persistentState;
        this.parent = parent;
        this.name = name;

        for (Entry<String, TransientNodeState> added : addedNodes.entrySet()) {
            String addedName = added.getKey();
            this.addedNodes.put(addedName,
                    new TransientNodeState(added.getValue(), this, addedName));
        }

        this.removedNodes.addAll(state.removedNodes);
        this.addedProperties.putAll(state.addedProperties);
        this.removedProperties.addAll(state.removedProperties);
    }

    public PropertyState getProperty(String name) {
        PropertyState state = addedProperties.get(name);
        if (state != null) {
            return state;
        }

        return removedProperties.contains(name)
            ? null
            : persistentState.getProperty(name);
    }

    public TransientNodeState getChildNode(String name) {
        TransientNodeState state = addedNodes.get(name);
        if (state != null) {
            return state;
        }

        return removedNodes.contains(name)
            ? null
            : getExistingChildNode(name);
    }

    public boolean hasNode(String name) {
        return getChildNode(name) != null;
    }

    public Iterable<PropertyState> getProperties() {
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

    public Iterable<TransientNodeState> getChildNodes(long offset, int count) {
        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedNodes);

        final Map<String, TransientNodeState> added = new HashMap<String, TransientNodeState>();
        added.putAll(addedNodes);

        final Iterable<? extends ChildNodeEntry>
                persistedNodes = persistentState.getChildNodeEntries(offset, count);

        return new Iterable<TransientNodeState>() {
            @Override
            public Iterator<TransientNodeState> iterator() {
                return new Iterator<TransientNodeState>() {
                    private final Iterator<? extends ChildNodeEntry>
                            nodes = persistedNodes.iterator();
                    private TransientNodeState next;

                    @Override
                    public boolean hasNext() {
                        if (next == null) {
                            while (nodes.hasNext()) {
                                final ChildNodeEntry entry = nodes.next();
                                if (added.containsKey(entry.getName())) {
                                    next = added.get(entry.getName());
                                    break;
                                }
                                if (!removed.contains(entry.getName())) {
                                    next = getExistingChildNode(entry.getName());
                                    break;
                                }
                            }
                        }
                        return next != null;
                    }

                    @Override
                    public TransientNodeState next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        TransientNodeState e = next;
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
        TransientNodeState state = getChildNode(name);
        removeNode(name);

        state.name = destName;
        state.parent = destParent;
        destParent.addedNodes.put(destName, state);
    }

    void copy(String name, TransientNodeState destParent, String destName) {
        destParent.addedNodes.put(destName,
                new TransientNodeState(getChildNode(name), destParent, destName));
    }

    private TransientNodeState getExistingChildNode(String name) {
        NodeState state = persistentState.getChildNode(name);

        TransientNodeState transientState = existingChildNodes.get(state);
        if (transientState == null) {
            transientState = new TransientNodeState(editor, name, state);
            existingChildNodes.put(state, transientState);
        }
        return transientState;
    }

}
