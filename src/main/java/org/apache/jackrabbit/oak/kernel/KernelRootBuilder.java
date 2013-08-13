package org.apache.jackrabbit.oak.kernel;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.PostCommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

/**
 * This implementation tracks the number of pending changes and purges them to
 * a private branch of the underlying store if a certain threshold is met.
 */
class KernelRootBuilder extends MemoryNodeBuilder {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the private branch.
     */
    private static final int UPDATE_LIMIT = Integer.getInteger("update.limit", 1000);

    /**
     * The underlying store
     */
    private final NodeStore store;

    /**
     * Private branch used to hold pending changes exceeding {@link #UPDATE_LIMIT}
     */
    private NodeStoreBranch branch;

    /**
     * Number of updated not yet persisted to the private {@link #branch}
     */
    private int updates = 0;

    public KernelRootBuilder(KernelNodeState base, KernelNodeStore store) {
        super(checkNotNull(base));
        this.store = store;
        this.branch = store.branch(base);
    }

    //--------------------------------------------------< MemoryNodeBuilder >---

    @Override
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new KernelNodeBuilder(this, name, this);
    }

    @Override
    protected void updated() {
        if (updates++ > UPDATE_LIMIT) {
            purge();
        }
    }

    //------------------------------------------------------------< internal >---

    /**
     * Rebase this builder on top of the head of the underlying store
     */
    NodeState rebase() {
        purge();
        branch.rebase();
        NodeState head = branch.getHead();
        reset(head);
        return head;
    }

    /**
     * Reset this builder by creating a new branch and setting the head
     * state of that branch as the new base state of this builder.
     */
    NodeState reset() {
        branch = store.branch();
        NodeState head = branch.getHead();
        reset(head);
        return head;
    }

    /**
     * Merge all changes tracked in this builder into the underlying store.
     */
    NodeState merge(CommitHook hook, PostCommitHook committed) throws CommitFailedException {
        purge();
        branch.merge(hook, committed);
        branch = store.branch();  // This keeps builders valid across merges (in contrast to branches)
        return branch.getHead();
    }

    /**
     * Applied all pending changes to the underlying branch and then
     * move the node as a separate operation on the underlying store.
     * This allows stores to optimise move operations instead of
     * seeing them as an added node followed by a deleted node.
     */
    boolean move(String source, String target) {
        purge();
        boolean success = branch.move(source, target);
        reset(branch.getHead());
        return success;
    }

    /**
     * Applied all pending changes to the underlying branch and then
     * copy the node as a separate operation on the underlying store.
     * This allows stores to optimise copy operations instead of
     * seeing them as an added node.
     */
    boolean copy(String source, String target) {
        purge();
        boolean success = branch.copy(source, target);
        reset(branch.getHead());
        return success;
    }

    private void purge() {
        branch.setRoot(getNodeState());
        reset(branch.getHead());
        updates = 0;
    }
}
