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
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import org.apache.commons.lang.StringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.LoggingCompositeHook;
import org.apache.jackrabbit.oak.upgrade.cli.node.TarNodeStore;
import org.apache.jackrabbit.oak.upgrade.nodestate.NameFilteringNodeState;
import org.apache.jackrabbit.oak.upgrade.nodestate.report.LoggingReporter;
import org.apache.jackrabbit.oak.upgrade.nodestate.report.ReportingNodeState;
import org.apache.jackrabbit.oak.upgrade.nodestate.NodeStateCopier;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;
import org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil;
import org.apache.jackrabbit.oak.upgrade.version.VersionableEditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.transform;
import static com.google.common.collect.Sets.union;
import static java.util.Collections.sort;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.NT_REP_PERMISSION_STORE;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_PERMISSION_STORE;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_EXCLUDE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_INCLUDE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.DEFAULT_MERGE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.calculateEffectiveIncludePaths;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.createIndexEditorProvider;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.createTypeEditorProvider;
import static org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade.markIndexesToBeRebuilt;
import static org.apache.jackrabbit.oak.upgrade.nodestate.NodeStateCopier.copyProperties;
import static org.apache.jackrabbit.oak.upgrade.version.VersionCopier.copyVersionStorage;
import static org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil.getVersionStorage;

public class RepositorySidegrade {

    private static final Logger LOG = LoggerFactory.getLogger(RepositorySidegrade.class);

    private static final int LOG_NODE_COPY = Integer.getInteger("oak.upgrade.logNodeCopy", 10000);

    private static final String WORKSPACE_NAME_PROP = "oak.upgrade.workspaceName";

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

    private boolean includeIndex = false;

    private boolean filterLongNames = true;

    private boolean verify = false;

    private boolean onlyVerify = false;

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
     * {@code <minDate, now()>}. <br>
     * <br>
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

    public void setIncludeIndex(boolean includeIndex) {
        this.includeIndex = includeIndex;
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

    public boolean isFilterLongNames() {
        return filterLongNames;
    }

    public void setFilterLongNames(boolean filterLongNames) {
        this.filterLongNames = filterLongNames;
    }

    public void setVerify(boolean verify) {
        this.verify = verify;
    }

    public void setOnlyVerify(boolean onlyVerify) {
        this.onlyVerify = onlyVerify;
    }

    /**
     * Same as {@link #copy(RepositoryInitializer)}, but with no custom initializer.
     *
     * @throws RepositoryException if the copy operation fails
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
     * @param initializer optional repository initializer to use
     *
     * @throws RepositoryException if the copy operation fails
     */
    public void copy(RepositoryInitializer initializer) throws RepositoryException {
        try {
            if (!onlyVerify) {
                NodeBuilder targetRoot = target.getRoot().builder();
                if (VersionHistoryUtil.getVersionStorage(targetRoot).exists() && !versionCopyConfiguration.skipOrphanedVersionsCopy()) {
                    LOG.warn("The version storage on destination already exists. Orphaned version histories will be skipped.");
                    versionCopyConfiguration.setCopyOrphanedVersions(null);
                }

                if (initializer != null) {
                    initializer.initialize(targetRoot);
                }

                final NodeState reportingSourceRoot = ReportingNodeState.wrap(source.getRoot(), new LoggingReporter(LOG, "Copying", LOG_NODE_COPY, -1));
                final NodeState sourceRoot;
                if (filterLongNames) {
                    sourceRoot = NameFilteringNodeState.wrap(reportingSourceRoot);
                } else {
                    sourceRoot = reportingSourceRoot;
                }
                copyState(sourceRoot, targetRoot);
            }
            if (verify || onlyVerify) {
                verify();
            }

        } catch (Exception e) {
            throw new RepositoryException("Failed to copy content", e);
        }
    }

    private void removeCheckpointReferences(NodeBuilder builder) throws CommitFailedException {
        // removing references to the checkpoints,
        // which don't exist in the new repository
        builder.setChildNode(":async");
    }

    private void copyState(NodeState sourceRoot, NodeBuilder targetRoot) throws CommitFailedException, RepositoryException {
        copyWorkspace(sourceRoot, targetRoot);

        if (includeIndex) {
            IndexCopier.copy(sourceRoot, targetRoot, includePaths);
        }

        boolean isRemoveCheckpointReferences = false;
        if (!isCompleteMigration()) {
            LOG.info("Custom paths have been specified, checkpoints won't be migrated");
            isRemoveCheckpointReferences = true;
        } else {
            boolean checkpointsCopied;
            try {
                checkpointsCopied = copyCheckpoints(targetRoot);
            } catch(UnsupportedOperationException e) {
                removeCheckpoints();
                checkpointsCopied = false;
                LOG.warn("Can't copy checkpoints without the access to the external blob store; migration will proceed");
            }
            if (!checkpointsCopied) {
                LOG.info("Copying checkpoints is not supported for this combination of node stores");
                isRemoveCheckpointReferences = true;
            }
        }
        if (isRemoveCheckpointReferences) {
            removeCheckpointReferences(targetRoot);
        }

        final List<CommitHook> hooks = new ArrayList<CommitHook>();
        if (!versionCopyConfiguration.isCopyAll()) {
            NodeBuilder versionStorage = VersionHistoryUtil.getVersionStorage(targetRoot);
            if (!versionStorage.exists()) { // it's possible that this is a new repository and the version storage
                                            // hasn't been created/copied yet
                versionStorage = VersionHistoryUtil.createVersionStorage(targetRoot);
            }
            if (!versionCopyConfiguration.skipOrphanedVersionsCopy()) {
                copyVersionStorage(targetRoot, getVersionStorage(sourceRoot), versionStorage, versionCopyConfiguration);
            }
            hooks.add(new EditorHook(new VersionableEditor.Provider(sourceRoot, getWorkspaceName(), versionCopyConfiguration)));
        }

        if (customCommitHooks != null) {
            hooks.addAll(customCommitHooks);
        }

        if (!isCompleteMigration()) {
            markIndexesToBeRebuilt(targetRoot);
            // type validation, reference and indexing hooks
            hooks.add(new EditorHook(new CompositeEditorProvider(
                    createTypeEditorProvider(),
                    createIndexEditorProvider()
            )));
        }

        target.merge(targetRoot, new LoggingCompositeHook(hooks, null, false), CommitInfo.EMPTY);
    }

    private boolean isCompleteMigration() {
        return includePaths.equals(DEFAULT_INCLUDE_PATHS) && excludePaths.equals(DEFAULT_EXCLUDE_PATHS) && mergePaths.equals(DEFAULT_MERGE_PATHS);
    }

    private void copyWorkspace(NodeState sourceRoot, NodeBuilder targetRoot) {
        final Set<String> includes = calculateEffectiveIncludePaths(includePaths, sourceRoot);
        final Set<String> excludes;
        if (versionCopyConfiguration.isCopyAll()) {
            excludes = copyOf(this.excludePaths);
        } else {
            excludes = union(copyOf(this.excludePaths), of("/jcr:system/jcr:versionStorage"));
        }
        final Set<String> merges = union(copyOf(this.mergePaths), of("/jcr:system"));

        NodeStateCopier.builder()
            .include(includes)
            .exclude(excludes)
            .merge(merges)
            .copy(sourceRoot, targetRoot);

        if (includePaths.contains("/")) {
            copyProperties(sourceRoot, targetRoot);
        }
    }

    private boolean copyCheckpoints(NodeBuilder targetRoot) {
        if (!(source instanceof TarNodeStore && target instanceof TarNodeStore)) {
            return false;
        }

        TarNodeStore sourceTarNS = (TarNodeStore) source;
        TarNodeStore targetTarNS = (TarNodeStore) target;

        NodeState sourceSuperRoot = sourceTarNS.getSuperRoot();
        NodeBuilder targetSuperRoot = targetTarNS.getSuperRoot().builder();

        String previousCheckpoint = null;
        for (String checkpoint : getCheckpointNames(sourceSuperRoot)) {
            NodeState targetPreviousRoot, sourcePreviousRoot;
            if (previousCheckpoint == null) {
                sourcePreviousRoot = source.getRoot();
                targetPreviousRoot = targetRoot.getNodeState();
            } else {
                sourcePreviousRoot = getCheckpointRoot(sourceSuperRoot, previousCheckpoint);
                targetPreviousRoot = getCheckpointRoot(targetSuperRoot.getNodeState(), previousCheckpoint);
            }
            NodeState sourceCheckpoint = getCheckpoint(sourceSuperRoot, checkpoint);
            NodeBuilder targetCheckpoint = getCheckpoint(targetSuperRoot, checkpoint);

            // copy checkpoint metadata
            NodeStateCopier.copyProperties(sourceCheckpoint, targetCheckpoint);
            targetCheckpoint.setChildNode("properties", sourceCheckpoint.getChildNode("properties"));

            // create the checkpoint root
            NodeState sourceCheckpointRoot = sourceCheckpoint.getChildNode("root");
            NodeBuilder targetCheckpointRoot = targetCheckpoint.setChildNode("root", targetPreviousRoot);
            sourceCheckpointRoot.compareAgainstBaseState(sourcePreviousRoot, new ApplyDiff(targetCheckpointRoot));

            previousCheckpoint = checkpoint;
        }

        targetTarNS.setSuperRoot(targetSuperRoot);
        return true;
   }

    private void removeCheckpoints() {
        if (!(target instanceof TarNodeStore)) {
            return;
        }
        TarNodeStore targetTarNS = (TarNodeStore) target;
        NodeBuilder targetSuperRoot = ((TarNodeStore) target).getSuperRoot().builder();
        targetSuperRoot.setChildNode("checkpoints");
        targetTarNS.setSuperRoot(targetSuperRoot);
    }

    /**
     * Return all checkpoint paths, sorted by their "created" property, descending.
     *
     * @param superRoot
     * @return
     */
    private static List<String> getCheckpointNames(NodeState superRoot) {
        List<ChildNodeEntry> checkpoints = newArrayList(superRoot.getChildNode("checkpoints").getChildNodeEntries().iterator());
        sort(checkpoints, new Comparator<ChildNodeEntry>() {
            @Override
            public int compare(ChildNodeEntry o1, ChildNodeEntry o2) {
                long c1 = o1.getNodeState().getLong("created");
                long c2 = o1.getNodeState().getLong("created");
                return -Long.compare(c1, c2);
            }
        });
        return transform(checkpoints, new Function<ChildNodeEntry, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ChildNodeEntry input) {
                return input.getName();
            }
        });
    }

    private static NodeState getCheckpointRoot(NodeState superRoot, String name) {
        return getCheckpoint(superRoot, name).getChildNode("root");
    }

    private static NodeState getCheckpoint(NodeState superRoot, String name) {
        return superRoot.getChildNode("checkpoints").getChildNode(name);
    }

    private static NodeBuilder getCheckpoint(NodeBuilder superRoot, String name) {
        return superRoot.child("checkpoints").child(name);
    }

    private String getWorkspaceName() throws RepositoryException {
        String definedName = System.getProperty(WORKSPACE_NAME_PROP);
        String detectedName = deriveWorkspaceName();
        if (StringUtils.isNotBlank(definedName)) {
            return definedName;
        } else if (StringUtils.isNotBlank(detectedName)) {
            return detectedName;
        } else {
            throw new RepositoryException("Can't detect the workspace name. Please use the system property " + WORKSPACE_NAME_PROP + " to set it manually.");
        }
    }

    /**
     * This method tries to derive the workspace name from the source repository. It uses the
     * fact that the /jcr:system/rep:permissionStore usually contains just one child
     * named after the workspace.
     *
     * @return the workspace name or null if it can't be derived
     */
    private String deriveWorkspaceName() {
        NodeState permissionStore = source.getRoot().getChildNode(JCR_SYSTEM).getChildNode(REP_PERMISSION_STORE);
        List<String> nameCandidates = new ArrayList<String>();
        for (ChildNodeEntry e : permissionStore.getChildNodeEntries()) {
            String primaryType = e.getNodeState().getName(JCR_PRIMARYTYPE);
            if (NT_REP_PERMISSION_STORE.equals(primaryType)) {
                nameCandidates.add(e.getName());
            }
        }
        if (nameCandidates.size() == 1) {
            return nameCandidates.get(0);
        } else {
            return null;
        }
    }

    private void verify() {
        final NodeState sourceRoot;
        final NodeState targetRoot;

        if (source instanceof TarNodeStore && target instanceof TarNodeStore) {
            sourceRoot = ((TarNodeStore) source).getSuperRoot();
            targetRoot = ((TarNodeStore) target).getSuperRoot();
        } else {
            sourceRoot = source.getRoot();
            targetRoot = target.getRoot();
        }

        final NodeState reportingSource = ReportingNodeState.wrap(sourceRoot, new LoggingReporter(LOG, "Verifying", LOG_NODE_COPY, -1));

        LOG.info("Verifying whether repositories are identical");
        if (targetRoot.compareAgainstBaseState(reportingSource, new LoggingEqualsDiff(LOG, "/"))) {
            LOG.info("Verification result: both repositories are identical");
        } else {
            LOG.warn("Verification result: repositories are not identical");
        }
    }
}
