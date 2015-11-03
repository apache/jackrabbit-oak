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
package org.apache.jackrabbit.oak.upgrade;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.LoggingCompositeHook;
import org.apache.jackrabbit.oak.upgrade.nodestate.report.LoggingReporter;
import org.apache.jackrabbit.oak.upgrade.nodestate.report.ReportingNodeState;
import org.apache.jackrabbit.oak.upgrade.nodestate.NodeStateCopier;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;
import org.apache.jackrabbit.oak.upgrade.version.VersionableEditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.union;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_EXCLUDE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_INCLUDE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_MERGE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.calculateEffectiveIncludePaths;
import static org.apache.jackrabbit.oak.upgrade.version.VersionCopier.copyVersionStorage;

public class RepositorySidegrade {

    private static final Logger LOG = LoggerFactory.getLogger(RepositorySidegrade.class);

    /**
     * Target node store.
     */
    private final NodeStore target;

    private final NodeStore source;

    /**
     * Paths to include during the copy process. Defaults to the root path "/".
     */
    private Set<String> includePaths = DEFAULT_INCLUDE_PATHS;

    /**
     * Paths to exclude during the copy process. Empty by default.
     */
    private Set<String> excludePaths = DEFAULT_EXCLUDE_PATHS;

    /**
     * Paths to merge during the copy process. Empty by default.
     */
    private Set<String> mergePaths = DEFAULT_MERGE_PATHS;

    private List<CommitHook> customCommitHooks = null;

    VersionCopyConfiguration versionCopyConfiguration = new VersionCopyConfiguration();

    /**
     * Configures the version storage copy. Be default all versions are copied.
     * One may disable it completely by setting {@code null} here or limit it to
     * a selected date range: {@code <minDate, now()>}.
     * 
     * @param minDate
     *            minimum date of the versions to copy or {@code null} to
     *            disable the storage version copying completely. Default value:
     *            {@code 1970-01-01 00:00:00}.
     */
    public void setCopyVersions(Calendar minDate) {
        versionCopyConfiguration.setCopyVersions(minDate);
    }

    /**
     * Configures copying of the orphaned version histories (eg. ones that are
     * not referenced by the existing nodes). By default all orphaned version
     * histories are copied. One may disable it completely by setting
     * {@code null} here or limit it to a selected date range:
     * {@code <minDate, now()>}. <br/>
     * <br/>
     * Please notice, that this option is overriden by the
     * {@link #setCopyVersions(Calendar)}. You can't copy orphaned versions
     * older than set in {@link #setCopyVersions(Calendar)} and if you set
     * {@code null} there, this option will be ignored.
     * 
     * @param minDate
     *            minimum date of the orphaned versions to copy or {@code null}
     *            to not copy them at all. Default value:
     *            {@code 1970-01-01 00:00:00}.
     */
    public void setCopyOrphanedVersions(Calendar minDate) {
        versionCopyConfiguration.setCopyOrphanedVersions(minDate);
    }

    /**
     * Creates a tool for copying the full contents of the source repository
     * to the given target repository. Any existing content in the target
     * repository will be overwritten.
     *
     * @param source source node store
     * @param target target node store
     */
    public RepositorySidegrade(NodeStore source, NodeStore target) {
        this.source = source;
        this.target = target;
    }

    /**
     * Returns the list of custom CommitHooks to be applied before the final
     * type validation, reference and indexing hooks.
     *
     * @return the list of custom CommitHooks
     */
    public List<CommitHook> getCustomCommitHooks() {
        return customCommitHooks;
    }

    /**
     * Sets the list of custom CommitHooks to be applied before the final
     * type validation, reference and indexing hooks.
     *
     * @param customCommitHooks the list of custom CommitHooks
     */
    public void setCustomCommitHooks(List<CommitHook> customCommitHooks) {
        this.customCommitHooks = customCommitHooks;
    }

    /**
     * Sets the paths that should be included when the source repository
     * is copied to the target repository.
     *
     * @param includes Paths to be included in the copy.
     */
    public void setIncludes(@Nonnull String... includes) {
        this.includePaths = copyOf(checkNotNull(includes));
    }

    /**
     * Sets the paths that should be excluded when the source repository
     * is copied to the target repository.
     *
     * @param excludes Paths to be excluded from the copy.
     */
    public void setExcludes(@Nonnull String... excludes) {
        this.excludePaths = copyOf(checkNotNull(excludes));
    }


    /**
     * Sets the paths that should be merged when the source repository
     * is copied to the target repository.
     *
     * @param merges Paths to be merged during copy.
     */
    public void setMerges(@Nonnull String... merges) {
        this.mergePaths = copyOf(checkNotNull(merges));
    }

    /**
     * Same as {@link #copy(RepositoryInitializer)}, but with no custom initializer. 
     */
    public void copy() throws RepositoryException {
        copy(null);
    }

    /**
     * Copies the full content from the source to the target repository.
     * <p>
     * The source repository <strong>must not be modified</strong> while
     * the copy operation is running to avoid an inconsistent copy.
     * <p>
     * Note that both the source and the target repository must be closed
     * during the copy operation as this method requires exclusive access
     * to the repositories.
     * 
     * @param initializer optional extra repository initializer to use
     *
     * @throws RepositoryException if the copy operation fails
     */
    public void copy(RepositoryInitializer initializer) throws RepositoryException {
        try {
            NodeState sourceRoot = source.getRoot();
            NodeBuilder targetRoot = target.getRoot().builder();

            new InitialContent().initialize(targetRoot);
            if (initializer != null) {
                initializer.initialize(targetRoot);
            }

            copyState(
                    ReportingNodeState.wrap(sourceRoot, new LoggingReporter(LOG, "Copying", 10000, -1)),
                    targetRoot
            );

        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private void removeCheckpointReferences(NodeBuilder builder) throws CommitFailedException {
        // removing references to the checkpoints, 
        // which don't exist in the new repository
        builder.setChildNode(":async");
    }

    private void copyState(NodeState sourceRoot, NodeBuilder targetRoot) throws CommitFailedException {
        copyWorkspace(sourceRoot, targetRoot);
        removeCheckpointReferences(targetRoot);
        if (!versionCopyConfiguration.skipOrphanedVersionsCopy()) {
            copyVersionStorage(sourceRoot, targetRoot, versionCopyConfiguration);
        }

        final List<CommitHook> hooks = new ArrayList<CommitHook>();
        hooks.add(new EditorHook(
                new VersionableEditor.Provider(sourceRoot, Oak.DEFAULT_WORKSPACE_NAME, versionCopyConfiguration)));

        if (customCommitHooks != null) {
            hooks.addAll(customCommitHooks);
        }


        target.merge(targetRoot, new LoggingCompositeHook(hooks, null, false), CommitInfo.EMPTY);
    }

    private void copyWorkspace(NodeState sourceRoot, NodeBuilder targetRoot) {
        final Set<String> includes = calculateEffectiveIncludePaths(includePaths, sourceRoot);
        final Set<String> excludes = union(copyOf(this.excludePaths), of("/jcr:system/jcr:versionStorage"));
        final Set<String> merges = union(copyOf(this.mergePaths), of("/jcr:system"));

        NodeStateCopier.builder()
            .include(includes)
            .exclude(excludes)
            .merge(merges)
            .copy(sourceRoot, targetRoot);
    }
}