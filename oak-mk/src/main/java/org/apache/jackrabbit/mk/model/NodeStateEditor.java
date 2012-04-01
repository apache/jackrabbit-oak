package org.apache.jackrabbit.mk.model;

/**
 * An editor for modifying existing and creating new
 * {@link NodeState node states}.
 */
public interface NodeStateEditor {

    /**
     * Add or replace the child node state with the given {@code name}.
     * @param name name of the new node state
     * @return editor for the added node state
     */
    NodeStateEditor addNode(String name);

    /**
     * Remove the child node state with the given {@code name}.
     * @param name  name of the node state to remove
     */
    void removeNode(String name);

    /**
     * Set a property on this node state
     * @param name name of the property
     * @param value value of the property
     */
    void setProperty(String name, Scalar value);

    /**
     * Remove a property from this node state
     * @param name name of the property
     */
    void removeProperty(String name);

    /**
     * Move the node state located at {@code sourcePath} to a node
     * state at {@code destPath}.
     * @param sourcePath source path relative to this node state
     * @param destPath destination path relative to this node state
     */
    void move(String sourcePath, String destPath);

    /**
     * Copy the node state located at {@code sourcePath} to a node
     * state at {@code destPath}.
     * @param sourcePath source path relative to this node state
     * @param destPath destination path relative to this node state
     */
    void copy(String sourcePath, String destPath);

    /**
     * Edit the child node state with the given {@code name}.
     * @param name name of the child node state to edit.
     * @return editor for the child node state of the given name or
     *         {@code null} if no such node state exists.
     */
    NodeStateEditor edit(String name);

    /**
     * Returns an immutable node state that matches the current state of
     * the editor.
     *
     * @return immutable node state
     */
    NodeState getNodeState();

    /**
     * Return the base node state of this private branch
     * @return base node state
     */
    NodeState getBaseNodeState();
}
