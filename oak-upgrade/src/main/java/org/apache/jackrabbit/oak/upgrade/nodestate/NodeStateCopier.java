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
package org.apache.jackrabbit.oak.upgrade.nodestate;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The NodeStateCopier and NodeStateCopier.Builder classes allow
 * recursively copying a NodeState to a NodeBuilder.
 * <br>
 * The copy algorithm is optimized for copying nodes between two
 * different NodeStore instances, i.e. where comparing NodeStates
 * is imprecise and/or expensive.
 * <br>
 * The algorithm does a post-order traversal. I.e. it copies
 * changed leaf-nodes first.
 * <br>
 * The work for a traversal without any differences between
 * {@code source} and {@code target} is equivalent to the single
 * execution of a naive equals implementation.
 */
public class NodeStateCopier {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStateCopier.class);


    private NodeStateCopier() {
        // no instances
    }

    /**
     * Shorthand method to copy one NodeStore to another. The changes in the
     * target NodeStore are automatically persisted.
     *
     * @param source NodeStore to copy from.
     * @param target NodeStore to copy to.
     * @throws CommitFailedException
     */
    public static boolean copyNodeStore(@Nonnull final NodeStore source, @Nonnull final NodeStore target)
            throws CommitFailedException {
        final NodeBuilder builder = checkNotNull(target).getRoot().builder();
        final boolean hasChanges = copyNodeState(checkNotNull(source).getRoot(), builder, "/", Collections.<String>emptySet());
        if (hasChanges) {
            source.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        return hasChanges;
    }

    /**
     * Copies all changed properties from the source NodeState to the target
     * NodeBuilder instance.
     *
     * @param source The NodeState to copy from.
     * @param target The NodeBuilder to copy to.
     * @return Whether changes were made or not.
     */
    public static boolean copyProperties(NodeState source, NodeBuilder target) {
        boolean hasChanges = false;

        // remove removed properties
        for (final PropertyState property : target.getProperties()) {
            final String name = property.getName();
            if (!source.hasProperty(name)) {
                target.removeProperty(name);
                hasChanges = true;
            }
        }

        // add new properties and change changed properties
        for (PropertyState property : source.getProperties()) {
            if (!property.equals(target.getProperty(property.getName()))) {
                target.setProperty(property);
                hasChanges = true;
            }
        }
        return hasChanges;
    }

    /**
     * Recursively copies the source NodeState to the target NodeBuilder.
     * <br>
     * Nodes that exist in the {@code target} but not in the {@code source}
     * are removed, unless they are descendants of one of the {@code mergePaths}.
     * This is determined by checking if the {@code currentPath} is a descendant
     * of any of the {@code mergePaths}.
     * <br>
     * <b>Note:</b> changes are not persisted.
     *
     * @param source NodeState to copy from
     * @param target NodeBuilder to copy to
     * @param currentPath The path of both the source and target arguments.
     * @param mergePaths A Set of paths under which existing nodes should be
     *                   preserved, even if the do not exist in the source.
     * @return An indication of whether there were changes or not.
     */
    public static boolean copyNodeState(@Nonnull final NodeState source, @Nonnull final NodeBuilder target,
                                        @Nonnull final String currentPath, @Nonnull final Set<String> mergePaths) {


        boolean hasChanges = false;

        // delete deleted children
        for (final String childName : target.getChildNodeNames()) {
            if (!source.hasChildNode(childName) && !isMerge(PathUtils.concat(currentPath, childName), mergePaths)) {
                target.setChildNode(childName, EmptyNodeState.MISSING_NODE);
                hasChanges = true;
            }
        }

        for (ChildNodeEntry child : source.getChildNodeEntries()) {
            final String childName = child.getName();
            final NodeState childSource = child.getNodeState();
            if (!target.hasChildNode(childName)) {
                // add new children
                target.setChildNode(childName, childSource);
                hasChanges = true;
            } else {
                // recurse into existing children
                final NodeBuilder childTarget = target.getChildNode(childName);
                final String childPath = PathUtils.concat(currentPath, childName);
                hasChanges = copyNodeState(childSource, childTarget, childPath, mergePaths) || hasChanges;
            }
        }

        hasChanges = copyProperties(source, target) || hasChanges;

        if (hasChanges) {
            LOG.trace("Node {} has changes", target);
        }

        return hasChanges;
    }

    private static boolean isMerge(String path, Set<String> mergePaths) {
        for (String mergePath : mergePaths) {
            if (PathUtils.isAncestor(mergePath, path) || mergePath.equals(path)) {
                return true;
            }
        }
        return false;
    }
}
