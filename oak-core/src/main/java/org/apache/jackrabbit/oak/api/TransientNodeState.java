package org.apache.jackrabbit.oak.api;

/**
 * A transient node state represents a node being edited. All edit operations are
 * done through an associated {@link org.apache.jackrabbit.oak.api.NodeStateEditor}.
 * <p>
 * A transient node state contains the current state of a node and is
 * in contrast to {@link org.apache.jackrabbit.mk.model.NodeState} instances
 * mutable and not thread safe.
 * <p>
 * The various accessors on this class mirror these of {@code NodeState}. However,
 * since instances of this class are mutable return values may change between
 * invocations.
 */
public interface TransientNodeState {
    /**
     * @return  the name of this transient node state
     */
    String getName();

    /**
     * @return  relative path of this transient node state
     */
    String getPath();

    /**
     * @return  the parent of this transient node state
     */
    TransientNodeState getParent();

    /**
     * @return  editor acting upon this instance
     */
    NodeStateEditor getEditor();

    /**
     * Get a property state
     * @param name name of the property state
     * @return  the property state with the given {@code name} or {@code null}
     *          if no such property state exists.
     */
    PropertyState getProperty(String name);

    /**
     * Determine if a property state exists
     * @param name  name of the property state
     * @return  {@code true} if and only if a property with the given {@code name}
     *          exists.
     */
    boolean hasProperty(String name);

    /**
     * Determine the number of properties.
     * @return  number of properties
     */
    long getPropertyCount();

    /**
     * Get a child node state
     * @param name  name of the child node state
     * @return  the child node state with the given {@code name} or {@code null}
     *          if no such child node state exists.
     */
    TransientNodeState getChildNode(String name);

    /**
     * Determine if a child node state exists
     * @param name  name of the child node state
     * @return  {@code true} if and only if a child node with the given {@code name}
     *          exists.
     */
    boolean hasNode(String name);

    /**
     * Determine the number of child nodes.
     * @return  number of child nodes.
     */
    long getChildNodeCount();

    /**
     * All property states. The returned {@code Iterable} has snapshot semantics. That
     * is, it reflect the state of this transient node state instance at the time of the
     * call. Later changes to this instance are no visible to iterators obtained from
     * the returned iterable.
     * @return  An {@code Iterable} for all property states
     */
    Iterable<PropertyState> getProperties();

    /**
     * All child node states. The returned {@code Iterable} has snapshot semantics. That
     * is, it reflect the state of this transient node state instance at the time of the
     * call. Later changes to this instance are no visible to iterators obtained from
     * the returned iterable.
     * @return  An {@code Iterable} for all child node states
     */
    Iterable<TransientNodeState> getChildNodes();
}
