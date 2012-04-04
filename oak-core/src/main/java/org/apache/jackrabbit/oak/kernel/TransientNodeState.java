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

/**
 * A transient node state represents a node being edited. All edit operations are
 * done through an associated {@link org.apache.jackrabbit.mk.model.NodeStateEditor}.
 * <p>
 * A transient node state contains the current state of a node and is
 * in contrast to {@link org.apache.jackrabbit.mk.model.NodeState} instances
 * mutable and not thread safe.
 * <p>
 * The various accessors on this class mirror these of {@code NodeState}. However,
 * since instances of this class are mutable return values may change between
 * invocations.
 */
public class TransientNodeState {
    /** Editor acting upon this instance */
    private final KernelNodeStateEditor editor;

    /**
     * Underlying persistent state or {@code null} if this instance represents an
     * added node state
     */
    private final NodeState persistentState;

    /** Resolved persistent child states */
    private final Map<String, TransientNodeState> existingChildNodes =
            new HashMap<String, TransientNodeState>();

    /** Transiently added node states */
    private final Map<String, TransientNodeState> addedNodes =
            new HashMap<String, TransientNodeState>();

    /** Transiently removed node stated */
    private final Set<String> removedNodes = new HashSet<String>();

    /** Transiently added property states */
    private final Map<String, PropertyState> addedProperties = new HashMap<String, PropertyState>();

    /** Transiently removed property states */
    private final Set<String> removedProperties = new HashSet<String>();

    /** Name of this state */
    private String name;

    /** Parent of this state */
    private TransientNodeState parent;

    /**
     * Create a new instance representing the root of a sub-tree.
     * @param persistentState  underlying persistent state
     * @param editor  editor acting upon the transient node state
     */
    TransientNodeState(NodeState persistentState, KernelNodeStateEditor editor) {
        this.editor = editor;
        this.persistentState = persistentState;
        this.parent = null;
        this.name = "";
    }

    /**
     * Create a new instance representing a added node state
     * @param parentEditor  editor of the parent state
     * @param name  name of the state
     */
    private TransientNodeState(KernelNodeStateEditor parentEditor, String name) {
        this(parentEditor, name, null);
    }

    /**
     * Create a new instance with an underlying persistent state
     * @param parentEditor  editor of the parent state
     * @param name  name of the state
     * @param persistedState  underlying persistent state
     */
    private TransientNodeState(KernelNodeStateEditor parentEditor, String name,
            NodeState persistedState) {

        editor = new KernelNodeStateEditor(parentEditor, this);
        this.persistentState = persistedState;
        parent = parentEditor.getTransientState();
        this.name = name;
    }

    /**
     * Copy constructor: create a deep copy of the passed {@code state} with
     * the given {@code name} and {@code parent}.
     * @param state  state to copy
     * @param parent  parent of the copied state
     * @param name  name of the copied state
     */
    private TransientNodeState(TransientNodeState state, TransientNodeState parent,
            String name) {

        editor = new KernelNodeStateEditor(parent.getEditor(), this);
        persistentState = state.persistentState;
        this.parent = parent;
        this.name = name;

        // recursively copy all existing node states
        for (Entry<String, TransientNodeState> existing : existingChildNodes.entrySet()) {
            String existingName = existing.getKey();
            this.existingChildNodes.put(existingName,
                    new TransientNodeState(existing.getValue(), this, existingName));
        }
        
        // recursively copy all added node states
        for (Entry<String, TransientNodeState> added : addedNodes.entrySet()) {
            String addedName = added.getKey();
            this.addedNodes.put(addedName,
                    new TransientNodeState(added.getValue(), this, addedName));
        }

        this.removedNodes.addAll(state.removedNodes);
        this.addedProperties.putAll(state.addedProperties);
        this.removedProperties.addAll(state.removedProperties);
    }

    /**
     * Get a property state
     * @param name name of the property state
     * @return  the property state with the given {@code name} or {@code null}
     *          if no such property state exists.
     */
    public PropertyState getProperty(String name) {
        PropertyState state = addedProperties.get(name);
        if (state != null) {
            // Added or removed and re-added property
            return state;
        }

        // Existing property unless removed
        return removedProperties.contains(name) || persistentState == null
            ? null
            : persistentState.getProperty(name);
    }

    /**
     * Determine if a property state exists
     * @param name  name of the property state
     * @return  {@code true} if and only if a property with the given {@code name}
     *          exists.
     */
    public boolean hasProperty(String name) {
        return getProperty(name) != null;
    }

    /**
     * Determine the number of properties.
     * @return  number of properties
     */
    public long getPropertyCount() {
        long persistentCount = persistentState == null
            ? 0
            : persistentState.getPropertyCount();
        
        return persistentCount + addedProperties.size() - removedProperties.size();
    }

    /**
     * Get a child node state
     * @param name  name of the child node state
     * @return  the child node state with the given {@code name} or {@code null}
     *          if no such child node state exists.
     */
    public TransientNodeState getChildNode(String name) {
        TransientNodeState state = addedNodes.get(name);
        if (state != null) {
            // Added or removed and re-added child node
            return state;
        }

        // Existing child node unless removed
        return removedNodes.contains(name)
            ? null
            : getExistingChildNode(name);
    }

    /**
     * Determine if a child node state exists
     * @param name  name of the child node state
     * @return  {@code true} if and only if a child node with the given {@code name}
     *          exists.
     */
    public boolean hasNode(String name) {
        return getChildNode(name) != null;
    }

    /**
     * Determine the number of child nodes.
     * @return  number of child nodes.
     */
    public long getChildNodeCount() {
        long persistentCount = persistentState == null
                ? 0
                : persistentState.getChildNodeCount();

        return persistentCount + addedNodes.size() - removedNodes.size();
    }

    /**
     * All property states. The returned {@code Iterable} has snapshot semantics. That
     * is, it reflect the state of this transient node state instance at the time of the
     * call. Later changes to this instance are no visible to iterators obtained from
     * the returned iterable.
     * @return  An {@code Iterable} for all property states
     */
    public Iterable<PropertyState> getProperties() {
        // Persisted property states
        final Iterable<? extends PropertyState> persisted = persistentState == null
                ? null
                : persistentState.getProperties();

        // Copy of removed property states
        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedProperties);

        // Copy of added and re-added property stated
        final Set<PropertyState> added = new HashSet<PropertyState>();
        added.addAll(addedProperties.values());

        // Filter removed property states from persisted property states
        // and add added property states
        return new Iterable<PropertyState>() {
            @Override
            public Iterator<PropertyState> iterator() {
                // persisted states
                Iterator<? extends PropertyState> properties =
                    persisted == null
                        ? Iterators.<PropertyState>empty()
                        : persisted.iterator();

                // persisted states - removed states
                Iterator<PropertyState> persistedMinusRemoved =
                    filter(properties, new Predicate<PropertyState>() {
                        @Override
                        public boolean evaluate(PropertyState state) {
                            return !removed.contains(state.getName());
                        }
                });

                // persisted states - removed states + added states
                return add(persistedMinusRemoved, added.iterator());
            }
        };
    }

    /**
     * All child node states. The returned {@code Iterable} has snapshot semantics. That
     * is, it reflect the state of this transient node state instance at the time of the
     * call. Later changes to this instance are no visible to iterators obtained from
     * the returned iterable.
     * @return  An {@code Iterable} for all child node states
     */
    public Iterable<TransientNodeState> getChildNodes(long offset, int count) {
        // Persisted child node states
        final Iterable<? extends ChildNodeEntry> persisted = persistentState == null
            ? null
            : persistentState.getChildNodeEntries(offset, count);

        // Copy od removed child node states
        final Set<String> removed = new HashSet<String>();
        removed.addAll(removedNodes);

        // Copy od added and re-added child node states
        final Set<TransientNodeState> added = new HashSet<TransientNodeState>();
        added.addAll(addedNodes.values());

        // Filter removed child node entries from persisted child node entries,
        // map remaining child node entries to child node states and add added
        // child node states
        return new Iterable<TransientNodeState>() {
            @Override
            public Iterator<TransientNodeState> iterator() {
                // persisted entries
                Iterator<? extends ChildNodeEntry> nodes = persisted == null
                    ? Iterators.<ChildNodeEntry>empty()
                    : persisted.iterator();

                // persisted entries - removed entries
                Iterator<ChildNodeEntry> persistedMinusRemovedEntries =
                    filter(nodes, new Predicate<ChildNodeEntry>() {
                        @Override
                        public boolean evaluate(ChildNodeEntry entry) {
                            return !removed.contains(entry.getName());
                        }
                });

                // persisted states - removed states
                Iterator<TransientNodeState> persistedMinusRemoved =
                    map(persistedMinusRemovedEntries,
                        new Function1<ChildNodeEntry, TransientNodeState>() {
                            @Override
                            public TransientNodeState apply(ChildNodeEntry entry) {
                                return getExistingChildNode(entry.getName());
                            }
                });

                // persisted states - removed states + added states
                return add(persistedMinusRemoved, added.iterator());
            }
        };
    }

    //------------------------------------------------------------< internal >---

    /**
     * @return  editor acting upon this instance
     */
    KernelNodeStateEditor getEditor() {
        return editor;
    }

    /**
     * @return  relative path of this transient node state
     */
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

    /**
     * Add a new child node state with the given {@code name}.
     * The behaviour of this method is not defined if a node state with that
     * {@code name} already exists.
     * @param name  name of the child node state
     */
    void addNode(String name) {
        addedNodes.put(name, new TransientNodeState(editor, name));
    }

    /**
     * Remove the child node state with the given {@code name}.
     * Does nothing if there is no node state with the given {@code name}.
     * @param name  name of the child node state
     */
    void removeNode(String name) {
        addedNodes.remove(name);
        if (hasExistingNode(name)) {
            // Mark as removed if removing existing
            removedNodes.add(name);
        }
    }

    /**
     * Set a property state.
     * @param state  a property state
     */
    void setProperty(PropertyState state) {
        addedProperties.put(state.getName(), state);
    }

    /**
     * Remove the property state with the given {@code name}.
     * Does nothing if there is no property state with the given {@code name}.
     * @param name  a property state
     */
    void removeProperty(String name) {
        addedProperties.remove(name);
        if (hasExistingProperty(name)) {
            // Mark as removed if removing existing
            removedProperties.add(name);
        }
    }

    /**
     * Move the child node state with the given {@code name} to the new parent at
     * The behaviour of this method is undefined if either this node state has
     * no child node state with the given {@code name} or {@code destParent} already
     * has a child node state of {@code destName}.
     *
     * @param name  name of the child node state to move
     * @param destParent  parent of the moved node state
     * @param destName  name of the moved node state
     */
    void move(String name, TransientNodeState destParent, String destName) {
        TransientNodeState state = getChildNode(name);
        removeNode(name);

        state.name = destName;
        state.parent = destParent;
        destParent.addedNodes.put(destName, state);
    }

    /**
     * Copy the child node state with the given {@code name} to the new parent at
     * The behaviour of this method is undefined if {@code destParent} already
     * has a child node state of {@code destName}.
     *
     * @param name  name of the child node state to move
     * @param destParent  parent of the moved node state
     * @param destName  name of the moved node state
     */
    void copy(String name, TransientNodeState destParent, String destName) {
        destParent.addedNodes.put(destName,
                new TransientNodeState(getChildNode(name), destParent, destName));
    }

    /**
     * Get a transient node state for a child node state which has
     * an existing underlying persistent node date.
     *
     * @param name  name of the child node state
     * @return  transient node state or {@code null} if this transient
     *          node state does not have an underlying persistent state
     *          or the underlying persistent state does not have a child
     *          node state with the given {@code name}.
     */
    private TransientNodeState getExistingChildNode(String name) {
        if (persistentState == null) {
            return null;
        }

        TransientNodeState transientState = existingChildNodes.get(name);
        if (transientState == null) {
            NodeState state = persistentState.getChildNode(name);
            transientState = new TransientNodeState(editor, name, state);
            existingChildNodes.put(name, transientState);
        }
        return transientState;
    }

    /**
     * Determine whether there is an underling persistent state which has
     * a child node state with the given {@code name}.
     * @param name  name of the child node state.
     * @return  {@code true} if and only if this transient node state has an
     *          underlying persistent state which has a child node state with
     *          the given {@code name}.
     */
    private boolean hasExistingNode(String name) {
        return persistentState != null && persistentState.getChildNode(name) != null;
    }

    /**
     * Determine whether there is an underling persistent state which has
     * a property state with the given {@code name}.
     * @param name  name of the property state.
     * @return  {@code true} if and only if this transient node state has an
     *          underlying persistent state which has a property state with
     *          the given {@code name}.
     */
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
