/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.MoveDetector;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * {@code PostValidationHook} implementation responsible for keeping track of
 * nested CUGs to simplify evaluation. The information about the nested CUGs is
 * stored in a hidden property associated with the policy node.
 *
 * Note, that this hook does _not_ respect the configured supported paths
 * and keeps track of all CUG policies irrespective of their validity.
 * Consequently all optimization considering the nested CUG information must
 * also verify the supported paths.
 */
class NestedCugHook implements PostValidationHook, CugConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(NestedCugHook.class);

    private final Set<String> deletedCUGs = new HashSet<>();
    private final Set<String> moveSources = new HashSet<>();

    //-------------------------------------------------< PostValidationHook >---
    @NotNull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) {
        NodeBuilder builder = after.builder();
        after.compareAgainstBaseState(before, new Diff(before, builder));
        deletedCUGs.clear();
        moveSources.clear();
        return builder.getNodeState();
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public String toString() {
        return "NestedCugHook";
    }

    //------------------------------------------------------------< private >---
    /**
     * Add the path containing a new CUG to the {@code HIDDEN_NESTED_CUGS} of the given {@code parentBuilder} node.
     * If such a property already existed additionally collect (and remove) those entries that are located below the
     * new CUG and thus need to be reconnected. At the same time paths pointing to a removed or moved nodes are
     * removed.
     *
     * If {@code setCugCnt} is {@code true}, {@code HIDDEN_TOP_CUG_CNT} will additionally be set accordingly.
     *
     * @param parentBuilder The node builder of the parent node
     * @param builder The current node build
     * @param pathWithNewCug That path of the node that got a new CUG policy added.
     * @param setCugCnt Returns {@code true} if an additional {@code HIDDEN_TOP_CUG_CNT} property must be subsequently set on the parent.
     */
    private void addNestedCugPath(@NotNull NodeBuilder parentBuilder, @NotNull NodeBuilder builder, @NotNull String pathWithNewCug, boolean setCugCnt) {
        PropertyState ps = parentBuilder.getProperty(HIDDEN_NESTED_CUGS);
        PropertyBuilder<String> pb = getHiddenPropertyBuilder(ps);
        if (ps != null) {
            List<String> moveToNestedCug = new ArrayList<>();
            for (String p : ps.getValue(Type.STRINGS)) {
                if (Text.isDescendant(pathWithNewCug, p)) {
                    pb.removeValue(p);
                    moveToNestedCug.add(p);
                } else if (p.equals(pathWithNewCug)) {
                    // already present with parent -> remove to avoid duplicate entries
                    log.debug("Path of node holding a new nested CUG is already listed with the parent CUG.");
                    pb.removeValue(p);
                }
                // since 'move' is recorded as delete and subsequent add, make sure any stale path pointing to
                // to a moved tree containing nested CUGs is cleared.
                // removed from the :nestedCug property.
                if (isDeletedOrMoved(p)) {
                    pb.removeValue(p);
                }
            }
            if (!moveToNestedCug.isEmpty()) {
                PropertyBuilder<String> pb2 = getHiddenPropertyBuilder(builder.getProperty(HIDDEN_NESTED_CUGS));
                pb2.addValues(moveToNestedCug);
                builder.setProperty(pb2.getPropertyState());
            }
        }

        // update the nested-cug property of the parent
        pb.addValue(pathWithNewCug);
        parentBuilder.setProperty(pb.getPropertyState());
        if (setCugCnt) {
            parentBuilder.setProperty(HIDDEN_TOP_CUG_CNT, (long) pb.count(), Type.LONG);
        }
    }

    /**
     * Determine if the given {@code path} points to a node that has been removed or is located in a tree structure that
     * was the source of a move operation (and thus no longer exists).
     *
     * @param path The path of a node
     * @return {@code true} if the path points to a deleted or moved node.
     */
    private boolean isDeletedOrMoved(@NotNull String path) {
        if (deletedCUGs.contains(path)) {
            return true;
        }
        for (String moveSource : moveSources) {
            if (moveSource.equals(path) || PathUtils.isAncestor(moveSource, path)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Update the {@code HIDDEN_NESTED_CUGS} of the given {@code parentBuilder} node if the path to be removed is contained
     * therein. If {@code setCugCnt} is {@code true}, {@code HIDDEN_TOP_CUG_CNT} will be adjusted accordingly.
     * If the {@code toRemove} is not contained this method returns {@code false} without modifying the property.
     *
     * @param parentBuilder The node builder of the parent node
     * @param toRemove The path to be removed
     * @param toReconnect An iterable of paths to be reconnected.
     * @param setCugCnt If {@code true} the {@code HIDDEN_TOP_CUG_CNT} property needs to be adjusted accordingly
     * @return {@code true} if the {@code HIDDEN_NESTED_CUGS} property contained the path to be removed and was successfully
     * updated, {@code false} otherwise.
     */
    private static boolean removeNestedCugPath(@NotNull NodeBuilder parentBuilder, @NotNull String toRemove,
                                               @NotNull Iterable<String> toReconnect, boolean setCugCnt) {
        PropertyState ps = parentBuilder.getProperty(HIDDEN_NESTED_CUGS);
        PropertyBuilder<String> pb = getHiddenPropertyBuilder(ps);
        if (pb.hasValue(toRemove)) {
            pb.removeValue(toRemove);
            pb.addValues(toReconnect);
            if (pb.isEmpty()) {
                parentBuilder.removeProperty(HIDDEN_NESTED_CUGS);
                // also remove HIDDEN_TOP_CUG_CNT (call is ignored if property not existing)
                parentBuilder.removeProperty(HIDDEN_TOP_CUG_CNT);
            } else {
                parentBuilder.setProperty(pb.getPropertyState());
                if (setCugCnt) {
                    parentBuilder.setProperty(HIDDEN_TOP_CUG_CNT, (long) pb.count(), Type.LONG);
                }
            }
            return true;
        } else {
            log.debug("Parent CUG doesn't contain expected entry for removed nested CUG");
            return false;
        }
    }

    private static PropertyBuilder<String> getHiddenPropertyBuilder(@Nullable PropertyState ps) {
        return PropertyBuilder.copy(Type.STRING, ps).setName(HIDDEN_NESTED_CUGS).setArray();
    }

    private final class Diff extends DefaultNodeStateDiff {

        private final Diff parentDiff;
        private final boolean isRoot;

        private final String path;
        private final NodeState beforeState;

        private final NodeBuilder afterBuilder;
        private final boolean afterHoldsCug;

        private Diff(@NotNull NodeState rootBefore, @NotNull NodeBuilder rootAfter) {
            parentDiff = null;
            isRoot = true;
            path = PathUtils.ROOT_PATH;

            beforeState = rootBefore;

            afterBuilder = rootAfter;
            afterHoldsCug = CugUtil.hasCug(rootAfter);
        }

        private Diff(@NotNull Diff parentDiff, @NotNull String name, @Nullable NodeState before, @Nullable NodeBuilder after) {
            this.parentDiff = parentDiff;
            isRoot = false;
            path = PathUtils.concat(parentDiff.path, name);

            this.beforeState = before;

            afterBuilder = after;
            afterHoldsCug = CugUtil.hasCug(after);
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                return true;
            }
            // remember moved nodes in order to drop entries in HIDDEN_NESTED_CUGS properties pointing to any tree at/below source-path
            if (after.hasProperty(MoveDetector.SOURCE_PATH)) {
                moveSources.add(after.getString(MoveDetector.SOURCE_PATH));
            }
            if (!CugUtil.definesCug(name, after)) {
                after.compareAgainstBaseState(EMPTY_NODE, new Diff(this, name, null, afterBuilder.getChildNode(name)));
                return true;
            }

            if (isRoot) {
                PropertyState alt = afterBuilder.getProperty(HIDDEN_NESTED_CUGS);
                if (alt != null) {
                    NodeBuilder cugNode = afterBuilder.getChildNode(REP_CUG_POLICY);
                    cugNode.setProperty(alt);
                    afterBuilder.removeProperty(HIDDEN_NESTED_CUGS);
                    afterBuilder.removeProperty(HIDDEN_TOP_CUG_CNT);
                }
            } else {
                Diff diff = parentDiff;
                while (diff != null) {
                    if (diff.afterHoldsCug) {
                        NodeBuilder cugNode = diff.afterBuilder.getChildNode(REP_CUG_POLICY);
                        addNestedCugPath(cugNode, afterBuilder.getChildNode(REP_CUG_POLICY), path, false);
                        break;
                    } else if (diff.isRoot) {
                        addNestedCugPath(diff.afterBuilder, afterBuilder.getChildNode(name), path, true);
                    }
                    diff = diff.parentDiff;
                }
            }
            // no need to traverse down the CUG policy node.
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                return true;
            }
            // OAK-8855 - Restore :nestedCugs on parent if it is removed.
            if (CugUtil.definesCug(name, after)) {
                Diff diff = parentDiff;
                while (diff != null) {
                    if (diff.afterHoldsCug) {
                        NodeBuilder cugNode = diff.afterBuilder.getChildNode(REP_CUG_POLICY);
                        addNestedCugPath(cugNode, afterBuilder.getChildNode(REP_CUG_POLICY), path, false);
                    }
                    diff = diff.parentDiff;
                }
            }
            after.compareAgainstBaseState(before, new Diff(this, name, before, afterBuilder.getChildNode(name)));
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (NodeStateUtils.isHidden(name)) {
                return true;
            }
            if (!CugUtil.definesCug(name, before)) {
                EMPTY_NODE.compareAgainstBaseState(before, new Diff(this, name, before, null));
                return true;
            }
            
            deletedCUGs.add(path);
            // reconnect information about nested CUGs at a parent if
            // only the CUG got removed but the whole subtree including CUGs
            // are still present.
            Set<String> reconnect = getCugPathsToReconnect(before);
            if (isRoot) {
                if (!Iterables.isEmpty(reconnect)) {
                    afterBuilder.setProperty(HIDDEN_NESTED_CUGS, reconnect, Type.STRINGS);
                    afterBuilder.setProperty(HIDDEN_TOP_CUG_CNT, reconnect.size());
                }
            } else {
                Diff diff = parentDiff;
                while (diff != null) {
                    if (processDiff(diff, reconnect)) {
                        break;
                    }
                    diff = diff.parentDiff;
                }
            }
            // no need to traverse down the CUG policy node
            return true;
        }
        
        private boolean processDiff(@NotNull Diff diff, @NotNull Set<String> reconnect) {
            if (diff.afterHoldsCug) {
                // found an existing parent CUG
                NodeBuilder cugNode = diff.afterBuilder.getChildNode(REP_CUG_POLICY);
                if (removeNestedCugPath(cugNode, path, reconnect, false)) {
                    return true;
                }
            }
            if (CugUtil.hasCug(diff.beforeState)) {
                // parent CUG got removed -> no removal/reconnect required if current path is listed.
                NodeState cugNode = diff.beforeState.getChildNode(REP_CUG_POLICY);
                PropertyState ps = cugNode.getProperty(HIDDEN_NESTED_CUGS);
                if (ps != null && Iterables.contains(ps.getValue(Type.STRINGS), path)) {
                    log.debug("Nested cug property containing {} has also been removed; no reconnect required.", path);
                    return true;
                }
            }
            if (diff.isRoot && !removeNestedCugPath(diff.afterBuilder, path, reconnect, true)) {
                log.warn("Failed to updated nested CUG info for path '{}'.", path);
            }
            return false;
        }

        /**
         * Collect the those paths stored in the {@code HIDDEN_NESTED_CUGS} of the given {@code before} state, that need
         * to be reconnected (i.e. added to the HIDDEN_NESTED_CUGS of the nearest ancestor). It will filter out paths of
         * nodes that have been removed (i.e. contained in {@code deletedCUGs}) as well as paths that are no longer
         * descendants of the path associated with the current {@code afterBuilder} node (cleaning entries left over
         * from OAK-9059).
         *
         * @param before The cug-policy node state that has been removed
         * @return The set of paths contained in the {@code HIDDEN_NESTED_CUGS} of the removed {@code before} state, that
         * need to be added to the HIDDEN_NESTED_CUGS of the nearest ancestor.
         */
        private Set<String> getCugPathsToReconnect(@NotNull NodeState before) {
            Set<String> reconnect = new HashSet<>();
            if (afterBuilder != null) {
                for (String nestedCug : before.getStrings(HIDDEN_NESTED_CUGS)) {
                    if (!PathUtils.isAncestor(path, nestedCug)) {
                        log.debug("Nested CUG path '{}' is not a descendant of '{}'. Omitting from reconnect during CUG policy removal.", nestedCug, path);
                    } else if (deletedCUGs.contains(nestedCug)) {
                        log.debug("Ignoring removed CUG path '{}' from reconnection at {}.", nestedCug, path);
                    } else {
                        String relPath = PathUtils.relativize(path, nestedCug);
                        NodeState ns = NodeStateUtils.getNode(afterBuilder.getNodeState(), relPath);
                        if (CugUtil.hasCug(ns)) {
                            reconnect.add(nestedCug);
                            log.debug("Marked nested CUG path '{}' for reconnection at {}.", nestedCug, path);
                        } else {
                            log.debug("Listed nested CUG path '{}' no longer holds a policy. Omitting from reconnect during CUG policy removal at {}.", nestedCug, path);
                        }
                    }
                }
            }
            return reconnect;
        }
    }
}
