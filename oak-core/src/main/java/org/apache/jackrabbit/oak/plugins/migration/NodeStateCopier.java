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
package org.apache.jackrabbit.oak.plugins.migration;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static java.util.Collections.emptySet;

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
 * <br>
 * <b>Usage:</b> For most use-cases the Builder API should be
 * preferred. It allows setting {@code includePaths},
 * {@code excludePaths} and {@code mergePaths}.
 * <br>
 * <b>Include paths:</b> if include paths are set, only these paths
 * and their sub-trees are copied. Any nodes that are not within the
 * scope of an include path are <i>implicitly excluded</i>.
 * <br>
 * <b>Exclude paths:</b> if exclude paths are set, any nodes matching
 * or below the excluded path are not copied. If an excluded node does
 * exist in the target, it is removed (see also merge paths).
 * <b>Exclude fragments:</b> if exclude fragments are set, nodes with names
 * matching any of the fragments (and their subtrees) are not copied. If an
 * excluded node does exist in the target, it is removed.
 * <b>Merge paths:</b> if merge paths are set, any nodes matching or
 * below the merged path will not be deleted from target, even if they
 * are missing in (or excluded from) the source.
 * <b>Preserve on Target</b> if set to true the nodes on target under the included 
 * paths are not deleted and the merge paths property is ignored. If false 
 * the deletion rules default to the case for merge paths. 
 */
public class NodeStateCopier {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStateCopier.class);

    private final Set<String> includePaths;

    private final Set<String> excludePaths;

    private final Set<String> fragmentPaths;

    private final Set<String> excludeFragments;

    private final Set<String> mergePaths;

    private final boolean referenceableFrozenNodes;

    private final boolean preserveOnTarget;
    
    private final Consumer<String> newNodesConsumer;

    private NodeStateCopier(Set<String> includePaths,
                            Set<String> excludePaths,
                            Set<String> fragmentPaths,
                            Set<String> excludeFragments,
                            Set<String> mergePaths,
                            boolean referenceableFrozenNodes,
                            boolean preserveOnTarget,
                            Consumer<String> newNodesConsumer) {
        this.includePaths = includePaths;
        this.excludePaths = excludePaths;
        this.fragmentPaths = fragmentPaths;
        this.excludeFragments = excludeFragments;
        this.mergePaths = mergePaths;
        this.referenceableFrozenNodes = referenceableFrozenNodes;
        this.preserveOnTarget = preserveOnTarget;
        this.newNodesConsumer = newNodesConsumer;
    }

    /**
     * Create a NodeStateCopier.Builder.
     *
     * @return a NodeStateCopier.Builder
     * @see NodeStateCopier.Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Shorthand method to copy one NodeStore to another. The changes in the
     * target NodeStore are automatically persisted.
     *
     * @param source NodeStore to copy from.
     * @param target NodeStore to copy to.
     * @return true if the target has been modified
     * @throws CommitFailedException if the operation fails
     * @see NodeStateCopier.Builder#copy(NodeStore, NodeStore)
     */
    public static boolean copyNodeStore(@NotNull final NodeStore source, @NotNull final NodeStore target)
            throws CommitFailedException {
        return builder().copy(checkNotNull(source), checkNotNull(target));
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

    private boolean copyNodeState(@NotNull final NodeState sourceRoot, @NotNull final NodeBuilder targetRoot) {
        final NodeState wrappedSource = FilteringNodeState.wrap("/", sourceRoot, this.includePaths, this.excludePaths, this.fragmentPaths, this.excludeFragments, this.referenceableFrozenNodes);
        boolean hasChanges = false;
        for (String includePath : this.includePaths) {
            hasChanges = copyMissingAncestors(sourceRoot, targetRoot, includePath) || hasChanges;
            final NodeState sourceState = NodeStateUtils.getNode(wrappedSource, includePath);
            if (sourceState.exists()) {
                final NodeBuilder targetBuilder = getChildNodeBuilder(targetRoot, includePath);
                hasChanges = copyNodeState(sourceState, targetBuilder, includePath, this.mergePaths,
                    this.preserveOnTarget, this.newNodesConsumer) || hasChanges;
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
     * @param newNodesConsumer A newNodesConsumer for the node paths being added
     * @return An indication of whether there were changes or not.
     */
    private static boolean copyNodeState(@NotNull final NodeState source, @NotNull final NodeBuilder target,
        @NotNull final String currentPath, @NotNull final Set<String> mergePaths,
       final boolean preserveOnTarget, Consumer newNodesConsumer) {


        boolean hasChanges = false;

        // delete deleted children
        for (final String childName : target.getChildNodeNames()) {
            if (!preserveOnTarget && !source.hasChildNode(childName) && !isMerge(PathUtils.concat(currentPath,
                childName), mergePaths)) {
                target.setChildNode(childName, EmptyNodeState.MISSING_NODE);
                hasChanges = true;
            }
        }

        for (ChildNodeEntry child : source.getChildNodeEntries()) {
            final String childName = child.getName();
            final NodeState childSource = child.getNodeState();
            final String childPath = PathUtils.concat(currentPath, childName);
            if (!target.hasChildNode(childName)) {
                // add new children
                target.setChildNode(childName, childSource);
                newNodesConsumer.accept(childPath);
                hasChanges = true;
            } else {
                // recurse into existing children
                final NodeBuilder childTarget = target.getChildNode(childName);
                hasChanges = copyNodeState(childSource, childTarget, childPath, mergePaths, preserveOnTarget, newNodesConsumer) || hasChanges;
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

    /**
     * Ensure that all ancestors of {@code path} are present in {@code targetRoot}. Copies any
     * missing ancestors from {@code sourceRoot}.
     *
     * @param sourceRoot NodeState to copy from
     * @param targetRoot NodeBuilder to copy to
     * @param path The path along which ancestors should be copied.
     */
    private static boolean copyMissingAncestors(
            final NodeState sourceRoot, final NodeBuilder targetRoot, final String path) {
        NodeState current = sourceRoot;
        NodeBuilder currentBuilder = targetRoot;
        boolean hasChanges = false;
        for (String name : PathUtils.elements(path)) {
            if (current.hasChildNode(name)) {
                final boolean targetHasChild = currentBuilder.hasChildNode(name);
                current = current.getChildNode(name);
                currentBuilder = currentBuilder.child(name);
                if (!targetHasChild) {
                    hasChanges = copyProperties(current, currentBuilder) || hasChanges;
                }
            }
        }
        return hasChanges;
    }

    /**
     * Allows retrieving a NodeBuilder by path relative to the given root NodeBuilder.
     *
     * All NodeBuilders are created via {@link NodeBuilder#child(String)} and are thus
     * implicitly created.
     *
     * @param root The NodeBuilder to consider the root node.
     * @param path An absolute or relative path, which is evaluated as a relative path under the root NodeBuilder.
     * @return a NodeBuilder instance, never null
     */
    @NotNull
    private static NodeBuilder getChildNodeBuilder(@NotNull final NodeBuilder root, @NotNull final String path) {
        NodeBuilder child = root;
        for (String name : PathUtils.elements(path)) {
            child = child.child(name);
        }
        return child;
    }

    /**
     * The NodeStateCopier.Builder allows configuring a NodeState copy operation with
     * {@code includePaths}, {@code excludePaths} and {@code mergePaths}.
     * <br>
     * <b>Include paths</b> can define which paths should be copied from the source to the
     * target.
     * <br>
     * <b>Exclude paths</b> allow restricting which paths should be copied. This is
     * especially useful when there are individual nodes in an included path that
     * should not be copied.
     * <br>
     * By default copying will remove items that already exist in the target but do
     * not exist in the source. If this behaviour is undesired that is where merge
     * paths come in.
     * <br>
     * <b>Merge paths</b> dictate in which parts of the tree the copy operation should
     * be <i>additive</i>, i.e. the content from source is merged with the content
     * in the target. Nodes that are present in the target but not in the source are
     * then not deleted. However, in the case where nodes are present in both the source
     * and the target, the node from the source is copied with its properties and any
     * properties previously present on the target's node are lost.
     * <br>
     * Finally, using one of the {@code copy} methods, NodeStores or NodeStates can
     * be copied.
     */
    public static class Builder {

        private Set<String> includePaths = of("/");

        private Set<String> excludePaths = emptySet();

        private Set<String> fragmentPaths = emptySet();

        private Set<String> excludeFragments = emptySet();

        private Set<String> mergePaths = emptySet();

        private boolean referenceableFrozenNodes = true;
        
        private boolean preserveOnTarget;
        
        private Consumer<String> newNodesConsumer = path -> {};
        
        private Builder() {}


        /**
         * Set include paths.
         *
         * @param paths include paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder include(@NotNull Set<String> paths) {
            if (!checkNotNull(paths).isEmpty()) {
                this.includePaths = copyOf(paths);
            }
            return this;
        }

        /**
         * Convenience wrapper for {@link #include(Set)}.
         *
         * @param paths include paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder include(@NotNull String... paths) {
            return include(copyOf(checkNotNull(paths)));
        }

        /**
         * Set if preserve paths on target
         *
         * @param preserveOnTarget if paths on target are preserved if not present on source
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder preserve(@NotNull boolean preserveOnTarget) {
            this.preserveOnTarget = preserveOnTarget;
            return this;
        }
        
        /**
         * Set exclude paths.
         *
         * @param paths exclude paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder exclude(@NotNull Set<String> paths) {
            if (!checkNotNull(paths).isEmpty()) {
                this.excludePaths = copyOf(paths);
            }
            return this;
        }

        /**
         * Convenience wrapper for {@link #exclude(Set)}.
         *
         * @param paths exclude paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder exclude(@NotNull String... paths) {
            return exclude(copyOf(checkNotNull(paths)));
        }

        /**
         * Set fragment paths.
         *
         * @param paths fragment paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder supportFragment(@NotNull Set<String> paths) {
            if (!checkNotNull(paths).isEmpty()) {
                this.fragmentPaths = copyOf(paths);
            }
            return this;
        }

        /**
         * Convenience wrapper for {@link #supportFragment(Set)}.
         *
         * @param paths fragment paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder supportFragment(@NotNull String... paths) {
            return supportFragment(copyOf(checkNotNull(paths)));
        }

        /**
         * Set exclude fragments.
         *
         * @param fragments exclude fragments
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder excludeFragments(@NotNull Set<String> fragments) {
            if (!checkNotNull(fragments).isEmpty()) {
                this.excludeFragments = copyOf(fragments);
            }
            return this;
        }

        /**
         * Convenience wrapper for {@link #exclude(Set)}.
         *
         * @param fragments exclude fragments
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder excludeFragments(@NotNull String... fragments) {
            return exclude(copyOf(checkNotNull(fragments)));
        }

        /**
         * Set merge paths.
         *
         * @param paths merge paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder merge(@NotNull Set<String> paths) {
            if (!checkNotNull(paths).isEmpty()) {
                this.mergePaths = copyOf(paths);
            }
            return this;
        }

        /**
         * Convenience wrapper for {@link #merge(Set)}.
         *
         * @param paths merge paths
         * @return this Builder instance
         * @see NodeStateCopier#NodeStateCopier(Set, Set, Set, Set, Set, boolean, boolean, Consumer)
         */
        @NotNull
        public Builder merge(@NotNull String... paths) {
            return merge(copyOf(checkNotNull(paths)));
        }

        @NotNull
        public Builder withReferenceableFrozenNodes(boolean isReferenceable) {
            this.referenceableFrozenNodes = isReferenceable;
            return this;
        }

        /**
         * Set consumer for node additions.
         * 
         * @param consumer consumer to listen to path additions
         * @return this Builder instance
         */
        public Builder withNodeConsumer(@NotNull Consumer consumer) {
            this.newNodesConsumer = consumer;
            return this;
        }
        
        /**
         * Creates a NodeStateCopier to copy the {@code sourceRoot} NodeState to the
         * {@code targetRoot} NodeBuilder, using any include, exclude, merge paths and
         * consumer set on this NodeStateCopier.Builder.
         * <br>
         * It is the responsibility of the caller to persist any changes using e.g.
         * {@link NodeStore#merge(NodeBuilder, CommitHook, CommitInfo)}.
         *
         * @param sourceRoot NodeState to copy from
         * @param targetRoot NodeBuilder to copy to
         * @return true if there were any changes, false if sourceRoot and targetRoot represent
         *         the same content
         */
        public boolean copy(@NotNull final NodeState sourceRoot, @NotNull final NodeBuilder targetRoot) {
            final NodeStateCopier copier = new NodeStateCopier(includePaths, excludePaths, fragmentPaths,
                excludeFragments, mergePaths, referenceableFrozenNodes, preserveOnTarget, newNodesConsumer);
            return copier.copyNodeState(checkNotNull(sourceRoot), checkNotNull(targetRoot));
        }

        /**
         * Creates a NodeStateCopier to copy the {@code source} NodeStore to the
         * {@code target} NodeStore, using any include, exclude, merge paths and
         * consumer set on this NodeStateCopier.Builder.
         * <br>
         * Changes are automatically persisted with empty CommitHooks and CommitInfo
         * via {@link NodeStore#merge(NodeBuilder, CommitHook, CommitInfo)}.
         *
         * @param source NodeStore to copy from
         * @param target NodeStore to copy to
         * @return true if there were any changes, false if source and target represent
         *         the same content
         * @throws CommitFailedException if the copy operation fails
         */
        public boolean copy(@NotNull final NodeStore source, @NotNull final NodeStore target)
                throws CommitFailedException {
            final NodeBuilder targetRoot = checkNotNull(target).getRoot().builder();
            if (copy(checkNotNull(source).getRoot(), targetRoot)) {
                target.merge(targetRoot, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                return true;
            }
            return false;
        }
    }
}
