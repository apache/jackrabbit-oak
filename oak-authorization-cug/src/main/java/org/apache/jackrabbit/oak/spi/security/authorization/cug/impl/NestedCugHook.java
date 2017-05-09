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

import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.util.Text;
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

    private Set<String> deletedCUGs = Sets.newHashSet();

    //-------------------------------------------------< PostValidationHook >---
    @Nonnull
    @Override
    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
        NodeBuilder builder = after.builder();
        after.compareAgainstBaseState(before, new Diff(before, builder));
        deletedCUGs.clear();
        return builder.getNodeState();
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public String toString() {
        return "NestedCugHook";
    }

    //------------------------------------------------------------< private >---

    private static long addNestedCugPath(@Nonnull NodeBuilder parentBuilder, @Nonnull NodeBuilder builder, @Nonnull String pathWithNewCug) {
        PropertyState ps = parentBuilder.getProperty(HIDDEN_NESTED_CUGS);
        PropertyBuilder<String> pb = getHiddenPropertyBuilder(ps);
        if (ps != null) {
            List<String> moveToNestedCug = Lists.newArrayList();
            for (String p : ps.getValue(Type.STRINGS)) {
                if (Text.isDescendant(pathWithNewCug, p)) {
                    pb.removeValue(p);
                    moveToNestedCug.add(p);
                } else if (p.equals(pathWithNewCug)) {
                    // already present with parent -> remove to avoid duplicate entries
                    log.debug("Path of node holding a new nested CUG is already listed with the parent CUG.");
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
        return pb.count();
    }

    private static int removeNestedCugPath(@Nonnull NodeBuilder parentBuilder, @Nonnull String toRemove, @Nonnull Iterable<String> toReconnect) {
        PropertyState ps = parentBuilder.getProperty(HIDDEN_NESTED_CUGS);
        PropertyBuilder<String> pb = getHiddenPropertyBuilder(ps);
        if (pb.hasValue(toRemove)) {
            pb.removeValue(toRemove);
            pb.addValues(toReconnect);
            if (pb.isEmpty()) {
                parentBuilder.removeProperty(HIDDEN_NESTED_CUGS);
                return 0;
            } else {
                parentBuilder.setProperty(pb.getPropertyState());
                return pb.count();
            }
        } else {
            log.debug("Parent CUG doesn't contain expected entry for removed nested CUG");
            return -1;
        }
    }

    private static PropertyBuilder<String> getHiddenPropertyBuilder(@Nullable PropertyState ps) {
        return PropertyBuilder.copy(Type.STRING, ps).setName(HIDDEN_NESTED_CUGS).setArray();
    }

    private final class Diff extends DefaultNodeStateDiff {

        private final Diff parentDiff;
        private final boolean isRoot;

        private String path;
        private NodeState beforeState = null;

        private NodeBuilder afterBuilder;
        private boolean afterHoldsCug;

        private Diff(@Nonnull NodeState rootBefore, @Nonnull NodeBuilder rootAfter) {
            parentDiff = null;
            isRoot = true;
            path = PathUtils.ROOT_PATH;

            beforeState = rootBefore;

            afterBuilder = rootAfter;
            afterHoldsCug = CugUtil.hasCug(rootAfter);
        }

        private Diff(@Nonnull Diff parentDiff, @Nonnull String name, @Nullable NodeState before, @Nullable NodeBuilder after) {
            this.parentDiff = parentDiff;
            isRoot = false;
            path = PathUtils.concat(parentDiff.path, name);

            this.beforeState = before;

            afterBuilder = after;
            afterHoldsCug = CugUtil.hasCug(after);
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (!NodeStateUtils.isHidden(name)) {
                if (CugUtil.definesCug(name, after)) {
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
                                addNestedCugPath(cugNode, afterBuilder.getChildNode(REP_CUG_POLICY), path);
                                break;
                            } else if (diff.isRoot) {
                                long cnt = addNestedCugPath(diff.afterBuilder, afterBuilder.getChildNode(name), path);
                                diff.afterBuilder.setProperty(HIDDEN_TOP_CUG_CNT, cnt, Type.LONG);

                            }
                            diff = diff.parentDiff;
                        }
                    }
                    // no need to traverse down the CUG policy node.
                } else {
                    after.compareAgainstBaseState(EMPTY_NODE, new Diff(this, name, null, afterBuilder.getChildNode(name)));
                }
            }
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            if (!NodeStateUtils.isHidden(name)) {
                after.compareAgainstBaseState(before, new Diff(this, name, before, afterBuilder.getChildNode(name)));
            }
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (!NodeStateUtils.isHidden(name)) {
                if (CugUtil.definesCug(name, before)) {
                    deletedCUGs.add(path);
                    // reconnect information about nested CUGs at a parent if
                    // only the CUG got removed but the whole subtree including CUGs
                    // are still present.
                    Set<String> reconnect = Sets.newHashSet();
                    if (afterBuilder != null) {
                        for (String nestedCug : before.getStrings(HIDDEN_NESTED_CUGS)) {
                            if (!deletedCUGs.contains(nestedCug)) {
                                String relPath = PathUtils.relativize(path, nestedCug);
                                NodeState ns = NodeStateUtils.getNode(afterBuilder.getNodeState(), relPath);
                                if (CugUtil.hasCug(ns)) {
                                    reconnect.add(nestedCug);
                                }
                            }
                        }
                    }
                    if (isRoot) {
                        if (!Iterables.isEmpty(reconnect)) {
                            afterBuilder.setProperty(HIDDEN_NESTED_CUGS, reconnect, Type.STRINGS);
                            afterBuilder.setProperty(HIDDEN_TOP_CUG_CNT, reconnect.size());
                        }
                    } else {
                        Diff diff = parentDiff;
                        while (diff != null) {
                            if (diff.afterHoldsCug) {
                                // found an existing parent CUG
                                NodeBuilder cugNode = diff.afterBuilder.getChildNode(REP_CUG_POLICY);
                                if (removeNestedCugPath(cugNode, path, reconnect) > -1) {
                                    break;
                                }
                            }
                            if (CugUtil.hasCug(diff.beforeState)) {
                                // parent CUG got removed -> no removal/reconnect required if current path is listed.
                                NodeState cugNode = diff.beforeState.getChildNode(REP_CUG_POLICY);
                                PropertyState ps = cugNode.getProperty(HIDDEN_NESTED_CUGS);
                                if (ps != null && Iterables.contains(ps.getValue(Type.STRINGS), path)) {
                                    log.debug("Nested cug property containing " + path + " has also been removed; no reconnect required.");
                                    break;
                                }
                            }
                            if (diff.isRoot) {
                                long cnt = removeNestedCugPath(diff.afterBuilder, path, reconnect);
                                if (cnt < 0) {
                                    log.warn("Failed to updated nested CUG info for path '" + path + "'.");
                                } else if (cnt == 0) {
                                    diff.afterBuilder.removeProperty(HIDDEN_TOP_CUG_CNT);
                                } else {
                                    diff.afterBuilder.setProperty(HIDDEN_TOP_CUG_CNT, cnt, Type.LONG);
                                }
                            }
                            diff = diff.parentDiff;
                        }
                    }
                    // no need to traverse down the CUG policy node
                } else {
                    EMPTY_NODE.compareAgainstBaseState(before, new Diff(this, name, before, null));
                }
            }
            return true;
        }
    }
}