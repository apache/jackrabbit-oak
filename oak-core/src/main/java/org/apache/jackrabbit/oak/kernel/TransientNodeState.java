package org.apache.jackrabbit.oak.kernel;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.PropertyState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.jackrabbit.oak.kernel.TransientNodeState.Iterators.*;

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

        return removedProperties.contains(name) || persistentState == null
            ? null
            : persistentState.getProperty(name);
    }
    
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }
    
    public long getPropertyCount() {
        long persistentCount = persistentState == null
            ? 0
            : persistentState.getPropertyCount();
        
        return persistentCount + addedProperties.size() - removedProperties.size();
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
    
    public long getChildNodeCount() {
        long persistentCount = persistentState == null
                ? 0
                : persistentState.getChildNodeCount();

        return persistentCount + addedNodes.size() - removedNodes.size();
    }

    public Iterable<PropertyState> getProperties() {
        final Iterable<? extends PropertyState> persisted = persistentState == null
                ? null
                : persistentState.getProperties();

        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedProperties);

        final Set<PropertyState> added = new HashSet<PropertyState>();
        added.addAll(addedProperties.values());

        // persisted - removed + added
        return new Iterable<PropertyState>() {
            @Override
            public Iterator<PropertyState> iterator() {
                Iterator<? extends PropertyState> properties = persisted == null
                    ? Iterators.<PropertyState>empty()
                    : persisted.iterator();

                Iterator<PropertyState> persistedMinusRemoved =
                    filter(properties, new Predicate<PropertyState>() {
                        @Override
                        public boolean evaluate(PropertyState state) {
                            return !removed.contains(state.getName());
                        }
                });

                return add(persistedMinusRemoved, added.iterator());
            }
        };
    }

    public Iterable<TransientNodeState> getChildNodes(long offset, int count) {
        final Iterable<? extends ChildNodeEntry> persisted = persistentState == null
            ? null
            : persistentState.getChildNodeEntries(offset, count);

        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedNodes);

        final Set<TransientNodeState> added = new HashSet<TransientNodeState>();
        added.addAll(addedNodes.values());

        // persisted - removed + added
        return new Iterable<TransientNodeState>() {
            @Override
            public Iterator<TransientNodeState> iterator() {
                Iterator<? extends ChildNodeEntry> nodes = persisted == null
                    ? Iterators.<ChildNodeEntry>empty()
                    : persisted.iterator();

                Iterator<ChildNodeEntry> persistedMinusRemovedEntries =
                    filter(nodes, new Predicate<ChildNodeEntry>() {
                        @Override
                        public boolean evaluate(ChildNodeEntry entry) {
                            return !removed.contains(entry.getName());
                        }
                });

                Iterator<TransientNodeState> persistedMinusRemoved =
                    map(persistedMinusRemovedEntries,
                        new Function1<ChildNodeEntry, TransientNodeState>() {
                            @Override
                            public TransientNodeState apply(ChildNodeEntry entry) {
                                return getExistingChildNode(entry.getName());
                            }
                });

                return add(persistedMinusRemoved, added.iterator());
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
        if (hasExistingNode(name)) {
            removedNodes.add(name);
        }
    }

    void setProperty(PropertyState state) {
        addedProperties.put(state.getName(), state);
    }

    void removeProperty(String name) {
        addedProperties.remove(name);
        if (hasExistingProperty(name)) {
            removedProperties.add(name);
        }
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
        if (persistentState == null) {
            return null;
        }

        NodeState state = persistentState.getChildNode(name);
        TransientNodeState transientState = existingChildNodes.get(state);
        if (transientState == null) {
            transientState = new TransientNodeState(editor, name, state);
            existingChildNodes.put(state, transientState);
        }
        return transientState;
    }
    
    private boolean hasExistingNode(String name) {
        return persistentState != null && persistentState.getChildNode(name) != null;
    }

    private boolean hasExistingProperty(String name) {
        return persistentState != null && persistentState.getProperty(name) != null;
    }

    // TODO: move to a more suitable location
    static final class Iterators {
        private Iterators() { }

        /**
         * Returns an empty iterator of type {@code T}.
         *
         * @param <T>
         * @return
         */
        @SuppressWarnings("unchecked")
        public static <T> Iterator<T> empty() {
            return EmptyIterator.INSTANCE;
        }

        /**
         * Returns an iterator for the concatenation of {@code iterator1} and
         * {@code iterator2}.
         *
         * @param <T>
         * @param iterator1
         * @param iterator2
         * @return
         */
        @SuppressWarnings("unchecked")
        public static <T> Iterator<T> add(Iterator<? extends T> iterator1,
                Iterator<? extends T> iterator2) {

            return new IteratorChain(iterator1, iterator2);
        }

        /**
         * Returns an iterator containing only the elements from an original
         * {@code iterator} where the given {@code predicate} matches.
         *
         * @param <T>
         * @param iterator
         * @param predicate
         * @return
         */
        @SuppressWarnings("unchecked")
        public static <T> Iterator<T> filter(Iterator<? extends T> iterator,
                final Predicate<? super T> predicate) {

            return new FilterIterator(iterator, new org.apache.commons.collections.Predicate() {
                @Override
                public boolean evaluate(Object object) {
                    return predicate.evaluate((T) object);
                }
            });
        }

        /**
         * Returns an iterator with elements of an original  {@code iterator} mapped by
         * a function {@code f}.
         *
         * @param <T>
         * @param <R>
         * @param <S>
         * @param iterator
         * @param f
         * @return
         */
        @SuppressWarnings("unchecked")
        public static <T, R, S extends T> Iterator<R> map(Iterator<? extends T> iterator,
                final Function1<S, ? super R> f) {

            return new TransformIterator(iterator, new org.apache.commons.collections.Transformer() {
                @Override
                public Object transform(Object input) {
                    return f.apply((S) input);
                }
            });
        }

        /**
         * Type safe counter part of {@link org.apache.commons.collections.Predicate}.
         *
         * @param <T> type of values this predicate is defined on
         */
        interface Predicate<T> {
            boolean evaluate(T arg);
        }

        /**
         * Type safe counter part of {@link org.apache.commons.collections.Transformer}.
         *
         * @param <S>  argument type to transform from
         * @param <T>  result type to transform to
         */
        public interface Function1<S, T> {
            T apply(S argument);
        }
    }
}
