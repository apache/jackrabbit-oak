package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Conflict Handler that merges concurrent updates to
 * cq:lastRolledout by undoing an unwanted deletion of this property
 */
public class CqLastRolledoutConflictHandler extends DefaultThreeWayConflictHandler {

    private static final String CQ_LAST_ROLLEDOUT= "cq:lastRolledout";

    /**
     * Create a new {@code ConflictHandler} which always returns
     * {@code resolution}.
     *
     */
    public CqLastRolledoutConflictHandler() {
        super(Resolution.IGNORED);
    }

    @NotNull
    @Override
    public Resolution changeDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState base) {
        if (CQ_LAST_ROLLEDOUT.equals(ours.getName())) {
            // re-add deleted property
            return Resolution.OURS;
        }
        return Resolution.IGNORED;
    }


}