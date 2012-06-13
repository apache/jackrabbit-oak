package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.api.ConflictHandler;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This implementation of a {@link ConflictHandler} always returns the same resolution.
 * It can be used to implement default behaviour or as a base class for more specialised
 * implementations.
 */
public class DefaultConflictHandler implements ConflictHandler {

    /**
     * A {@code ConflictHandler} which always return {@link Resolution#OURS}.
     */
    public static final ConflictHandler OURS = new DefaultConflictHandler(Resolution.OURS);

    /**
     * A {@code ConflictHandler} which always return {@link Resolution#THEIRS}.
     */
    public static final ConflictHandler THEIRS = new DefaultConflictHandler(Resolution.THEIRS);

    private final Resolution resolution;

    /**
     * Create a new {@code ConflictHandler} which always returns {@code resolution}.
     *
     * @param resolution  the resolution to return from all methods of this
     * {@code ConflictHandler} instance.
     */
    public DefaultConflictHandler(Resolution resolution) {
        this.resolution = resolution;
    }

    @Override
    public Resolution addExistingProperty(Tree parent, PropertyState ours, PropertyState theirs) {
        return resolution;
    }

    @Override
    public Resolution changeDeletedProperty(Tree parent, PropertyState ours) {
        return resolution;
    }

    @Override
    public Resolution changeChangedProperty(Tree parent, PropertyState ours, PropertyState theirs) {
        return resolution;
    }

    @Override
    public Resolution deleteChangedProperty(Tree parent, PropertyState theirs) {
        return resolution;
    }

    @Override
    public Resolution addExistingNode(Tree parent, String name, NodeState ours, NodeState theirs) {
        return resolution;
    }

    @Override
    public Resolution changeDeletedNode(Tree parent, String name, NodeState ours) {
        return resolution;
    }

    @Override
    public Resolution deleteChangedNode(Tree parent, String name, NodeState theirs) {
        return resolution;
    }
}
