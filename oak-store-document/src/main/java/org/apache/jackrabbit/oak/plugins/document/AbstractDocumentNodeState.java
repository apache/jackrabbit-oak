/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.util.ReverseNodeStateDiff;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public abstract class AbstractDocumentNodeState extends AbstractNodeState {
    private static final PerfLogger perfLogger = new PerfLogger(
            LoggerFactory.getLogger(AbstractDocumentNodeState.class.getName()
                    + ".perf"));

    public abstract Path getPath();

    public abstract RevisionVector getLastRevision();

    public abstract RevisionVector getRootRevision();

    public abstract boolean isFromExternalChange();

    /**
     * Creates a copy of this {@code DocumentNodeState} with the
     * {@link #getRootRevision()} set to the given {@code root} revision. This method
     * returns {@code this} instance if the given {@code root} revision is
     * the same as the one in this instance and the {@link #isFromExternalChange()}
     * flags are equal.
     *
     * @param root the root revision for the copy of this node state.
     * @param externalChange if the {@link #isFromExternalChange()} flag must be
     *                       set on the returned node state.
     * @return a copy of this node state with the given root revision and
     *          external change flag.
     */
    public abstract AbstractDocumentNodeState withRootRevision(@NotNull RevisionVector root,
                                               boolean externalChange);

    public abstract boolean hasNoChildren();

    protected abstract NodeStateDiffer getNodeStateDiffer();

    //--------------------------< NodeState >-----------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof AbstractDocumentNodeState) {
            AbstractDocumentNodeState other = (AbstractDocumentNodeState) that;
            if (!getPath().equals(other.getPath())) {
                // path does not match: not equals
                // (even if the properties are equal)
                return false;
            }
            if (revisionEquals(other)) {
                return true;
            }
            // revision does not match: might still be equals
        } else if (that instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) that;
            NodeState baseState = modified.getBaseState();
            if (baseState instanceof ModifiedDocumentNodeState) {
                baseState = ((ModifiedDocumentNodeState) baseState).getBaseState();
            }
            if (baseState == this) {
                return EqualsDiff.equals(this, modified);
            }
        }
        if (that instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) that);
        }
        return false;
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base) {
            return true;
        } else if (base == EMPTY_NODE || !base.exists()) {
            // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (base instanceof AbstractDocumentNodeState) {
            AbstractDocumentNodeState mBase = (AbstractDocumentNodeState) base;
            if (getPath().equals(mBase.getPath())){
                if (revisionEquals(mBase)) {
                    // no differences
                    return true;
                } else {
                    // use DocumentNodeStore compare
                    final long start = perfLogger.start();
                    try {
                        return getNodeStateDiffer().compare(this, mBase, diff);
                    } finally {
                        if (start > 0) {
                            perfLogger
                                    .end(start,
                                            1,
                                            "compareAgainstBaseState, path={}, lastRevision={}, base.path={}, base.lastRevision={}",
                                            getPath(), getLastRevision(),
                                            mBase.getPath(), mBase.getLastRevision());
                        }
                    }
                }
            }
        } else if (base instanceof ModifiedNodeState) {
            ModifiedNodeState mBase = (ModifiedNodeState) base;
            if (mBase.getBaseState() == this) {
                // this is the base state of the ModifiedNodeState
                // do a reverse comparison and report the inverse back to NodeStateDiff
                return mBase.compareAgainstBaseState(this, new ReverseNodeStateDiff(diff));
            }
        }
        // fall back to the generic node state diff algorithm
        return super.compareAgainstBaseState(base, diff);
    }

    //------------------------------< internal >--------------------------------

    /**
     * Returns {@code true} if this state is equal to the {@code other} state
     * by inspecting the root and last revision. Two node states are guaranteed
     * to be equal if their root revisions are equal (even if the two revisions
     * have different branch flags) or their last revisions are equal. This
     * method may return {@code false} even if the actual states are in fact
     * equal!
     *
     * @param other the other state to compare with.
     * @return {@code true} if this state is equal to the {@code other} state
     *      based on the root and last revisions.
     */
    private boolean revisionEquals(AbstractDocumentNodeState other) {
        if (this.getRootRevision().asTrunkRevision()
                .equals(other.getRootRevision().asTrunkRevision())) {
            return true;
        }
        return this.getLastRevision() != null
                && this.getLastRevision().equals(other.getLastRevision());
    }

    /**
     * Configures the performance logger with the specified info log interval.
     *
     * @param infoLogMillis the interval in milliseconds for logging performance information.
     */
    static void configurePerfLogger(long infoLogMillis) {
        perfLogger.setInfoLogMillis(infoLogMillis);
    }
}
