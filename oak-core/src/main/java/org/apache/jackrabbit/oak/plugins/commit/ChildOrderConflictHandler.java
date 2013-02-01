package org.apache.jackrabbit.oak.plugins.commit;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;

/**
 * This conflict handler instance takes care of properly merging conflicts
 * occurring by concurrent reorder operations.
 *
 * @see TreeImpl#OAK_CHILD_ORDER
 */
public class ChildOrderConflictHandler extends ConflictHandlerWrapper {

    public ChildOrderConflictHandler(ConflictHandler handler) {
        super(handler);
    }

    @Override
    public Resolution addExistingProperty(NodeBuilder parent,
            PropertyState ours,
            PropertyState theirs) {
        if (isChildOrderProperty(ours)) {
            // two sessions concurrently called orderBefore() on a Tree
            // that was previously unordered.
            return Resolution.THEIRS;
        } else {
            return handler.addExistingProperty(parent, ours, theirs);
        }
    }

    @Override
    public Resolution changeDeletedProperty(NodeBuilder parent,
            PropertyState ours) {
        if (isChildOrderProperty(ours)) {
            // orderBefore() on trees that were deleted
            return Resolution.THEIRS;
        } else {
            return handler.changeDeletedProperty(parent, ours);
        }
    }

    @Override
    public Resolution changeChangedProperty(NodeBuilder parent,
            PropertyState ours,
            PropertyState theirs) {
        if (isChildOrderProperty(ours)) {
            merge(parent, ours, theirs);
            return Resolution.MERGED;
        } else {
            return handler.changeChangedProperty(parent, ours, theirs);
        }
    }

    private static void merge(NodeBuilder parent, PropertyState ours, PropertyState theirs) {
        Set<String> theirOrder = Sets.newHashSet(theirs.getValue(Type.STRINGS));
        PropertyBuilder<String> merged = MemoryPropertyBuilder.array(Type.STRING)
                .assignFrom(theirs);

        // Append child node names from ours that are not in theirs
        for (String ourChild : ours.getValue(Type.STRINGS)) {
            if (!theirOrder.contains(ourChild)) {
                merged.addValue(ourChild);
            }
        }

        // Remove child node names of nodes that have been removed
        for (String child : merged.getValues()) {
            if (!parent.hasChildNode(child)) {
                merged.removeValue(child);
            }
        }

        parent.setProperty(merged.getPropertyState());
    }

    @Override
    public Resolution deleteDeletedProperty(NodeBuilder parent,
            PropertyState ours) {
        if (isChildOrderProperty(ours)) {
            // concurrent remove of ordered trees
            return Resolution.THEIRS;
        } else {
            return handler.deleteDeletedProperty(parent, ours);
        }
    }

    @Override
    public Resolution deleteChangedProperty(NodeBuilder parent,
            PropertyState theirs) {
        if (isChildOrderProperty(theirs)) {
            // remove trees that were reordered by another session
            return Resolution.THEIRS;
        } else {
            return handler.deleteChangedProperty(parent, theirs);
        }
    }

    //----------------------------< internal >----------------------------------

    private static boolean isChildOrderProperty(PropertyState p) {
        return TreeImpl.OAK_CHILD_ORDER.equals(p.getName());
    }
}

