package org.apache.jackrabbit.oak.plugins.commit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This commit hook implementation is responsible for resolving
 * conflicts. It does so by detecting the presence of conflict
 * markers added by the Microkernel and delegating to a
 * {@link org.apache.jackrabbit.oak.spi.commit.ConflictHandler}
 * for resolving the conflicts.
 *
 * @see org.apache.jackrabbit.mk.api.MicroKernel#rebase(String, String)
 */
public class ConflictHook implements CommitHook {
    private final ConflictHandler conflictHandler;

    /**
     * Create a new instance of the conflict hook using the
     * passed conflict handler for resolving conflicts.
     * @param conflictHandler  a conflict handler
     */
    public ConflictHook(ConflictHandler conflictHandler) {
        this.conflictHandler = conflictHandler;
    }

    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after) throws CommitFailedException {
        return MergingNodeStateDiff.merge(before, after, conflictHandler);
    }
}
