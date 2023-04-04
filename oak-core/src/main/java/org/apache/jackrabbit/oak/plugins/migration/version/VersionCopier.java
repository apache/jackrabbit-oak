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
package org.apache.jackrabbit.oak.plugins.migration.version;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.plugins.migration.DescendantsIterator;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier;
import org.apache.jackrabbit.oak.plugins.version.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_ROOTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONLABELS;
import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getVersionHistoryBuilder;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;

import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getRelativeVersionHistoryPath;
import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getVersionHistoryLastModified;
import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getVersionHistoryNodeState;

/**
 * This class allows to copy the version history, optionally filtering it with a
 * given date.
 */
public class VersionCopier {

    private static final Logger logger = LoggerFactory.getLogger(VersionCopier.class);

    private final NodeState sourceVersionStorage;

    private final NodeBuilder targetVersionStorage;

    private final Supplier<Boolean> frozenNodeIsReferenceable;
    
    private final Consumer<String> consumer;

    public VersionCopier(NodeBuilder targetRoot, NodeState sourceVersionStorage, NodeBuilder targetVersionStorage) {
        this(targetRoot, sourceVersionStorage, targetVersionStorage, path -> {});
    }

    public VersionCopier(NodeBuilder targetRoot, NodeState sourceVersionStorage, NodeBuilder targetVersionStorage, Consumer<String> consumer) {
        this.sourceVersionStorage = sourceVersionStorage;
        this.targetVersionStorage = targetVersionStorage;
        this.frozenNodeIsReferenceable = new IsFrozenNodeReferenceable(targetRoot.getNodeState());
        this.consumer = consumer;
    }

    public static void copyVersionStorage(NodeBuilder targetRoot, NodeState sourceVersionStorage, NodeBuilder targetVersionStorage, VersionCopyConfiguration config) {
        copyVersionStorage(targetRoot, sourceVersionStorage, targetVersionStorage, config, path -> {});
    }
    
    public static void copyVersionStorage(NodeBuilder targetRoot, NodeState sourceVersionStorage, NodeBuilder targetVersionStorage, VersionCopyConfiguration config,
       @NotNull Consumer<String> consumer) {
        final Iterator<NodeState> versionStorageIterator = new DescendantsIterator(sourceVersionStorage, 3);
        final VersionCopier versionCopier = new VersionCopier(targetRoot, sourceVersionStorage, targetVersionStorage, consumer);

        while (versionStorageIterator.hasNext()) {
            final NodeState versionHistoryBucket = versionStorageIterator.next();
            for (String versionHistory : versionHistoryBucket.getChildNodeNames()) {
                if (config.removeTargetVersionHistory()) {
                    getVersionHistoryBuilder(targetVersionStorage, versionHistory).remove();
                }
                versionCopier.copyVersionHistory(versionHistory, config.getOrphanedMinDate(),
                    config.preserveOnTarget());
            }
        }
    }

    /**
     * Copy history filtering versions using passed date and returns {@code
     * true} if the history has been copied.
     * If preserveOnTarget is true then only copies non-conflicting versions.
     * 
     * @param versionableUuid
     *            Name of the version history node
     * @param minDate
     *            Only versions older than this date will be copied
     * @param preserveOnTarget
     *             Preserve version not present on target
     * @return {@code true} if at least one version has been copied
     */
    public boolean copyVersionHistory(String versionableUuid, Calendar minDate, boolean preserveOnTarget) {
        final String versionHistoryPath = getRelativeVersionHistoryPath(versionableUuid);
        final NodeState sourceVersionHistory = getVersionHistoryNodeState(sourceVersionStorage, versionableUuid);
        final Calendar lastModified = getVersionHistoryLastModified(sourceVersionHistory);

        if (sourceVersionHistory.exists() && (lastModified.after(minDate) || minDate.getTimeInMillis() == 0) &&
            hasNoConflicts(versionHistoryPath, versionableUuid, preserveOnTarget, sourceVersionHistory)) {
            NodeStateCopier.builder()
                    .include(versionHistoryPath)
                    .merge(VERSION_STORE_PATH)
                    .withReferenceableFrozenNodes(frozenNodeIsReferenceable.get())
                    .preserve(preserveOnTarget)
                    .withNodeConsumer(consumer)
                    .copy(sourceVersionStorage, targetVersionStorage);
            return true;
        }
        return false;
    }

    private boolean hasNoConflicts(String versionHistoryPath, String versionableUuid, boolean preserveOnTarget, NodeState sourceVersionHistory) {
        // if preserveOnTarget is true then check no conflicts which means version history has moved forward only
        if (preserveOnTarget) {
            NodeBuilder targetVersionHistory = getVersionHistoryBuilder(targetVersionStorage, versionableUuid);
            if (targetVersionHistory.exists()) {
                VersionComparator versionComparator = new VersionComparator();

                // version history id not equal
                boolean conflictingVersionHistory =
                     !Objects.equals(targetVersionHistory.getString(JCR_UUID), sourceVersionHistory.getString(JCR_UUID));
                if (conflictingVersionHistory) {
                    logger.info("Skipping version history for {}: Conflicting version history found",
                        versionHistoryPath);
                    return false;
                }

                // Get the version names except jcr:rootVersion
                List<String> targetVersions =
                    StreamSupport.stream(targetVersionHistory.getChildNodeNames().spliterator(), false).filter(s -> !s.equals(JCR_ROOTVERSION) && !s.equals(JCR_VERSIONLABELS))
                        .sorted(versionComparator).collect(Collectors.toList());
                List<String> sourceVersions =
                    StreamSupport.stream(sourceVersionHistory.getChildNodeNames().spliterator(), false).filter(s -> !s.equals(JCR_ROOTVERSION) && !s.equals(JCR_VERSIONLABELS))
                        .sorted(versionComparator).collect(Collectors.toList());
                // source version only has a rootVersion which means nothing to update
                boolean noUpdate = sourceVersions.isEmpty() || targetVersions.containsAll(sourceVersions);
                if (noUpdate) {
                    logger.info("Skipping version history for {}: No update required", versionHistoryPath);
                    return false;
                }

                // highest source version does not exist on target or
                // all source versions already exist on target (diverged or no diff)
                boolean diverged = !targetVersions.contains(sourceVersions.get(0));
                if (diverged) {
                    logger.info("Skipping version history for {}: Versions diverged", versionHistoryPath);
                    return false;
                }

                // highest source version UUID does not match the corresponding version on target (diverged)
                boolean conflictingHighestVersion =
                    !Objects.equals(sourceVersionHistory.getChildNode(sourceVersions.get(0)).getString(JCR_UUID), 
                        targetVersionHistory.getChildNode(sourceVersions.get(0)).getString(JCR_UUID));
                if (conflictingHighestVersion) {
                    logger.info("Skipping version history for {}: Old base version id changed", versionHistoryPath);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Descending numeric versions comparator
     */
    static class VersionComparator implements Comparator<String> {
        @Override
        public int compare(String v1, String v2) {
            String[] v1Seg = v1.split("\\.");
            String[] v2Seg = v2.split("\\.");
            Iterator<String> i1 = Arrays.asList(v1Seg).iterator();
            Iterator<String> i2 = Arrays.asList(v2Seg).iterator();
            while (i1.hasNext() && i2.hasNext()) {
                int cmp = Integer.compare(Integer.parseInt(i2.next()), Integer.parseInt(i1.next()));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(v2Seg.length, v1Seg.length);
        }
    }

    private static final class IsFrozenNodeReferenceable implements Supplier<Boolean> {

        private final NodeState root;

        private Boolean isReferenceable;

        public IsFrozenNodeReferenceable(NodeState root) {
            this.root = root;
        }

        @Override
        public Boolean get() {
            if (isReferenceable == null) {
                isReferenceable = Utils.isFrozenNodeReferenceable(root);
            }
            return isReferenceable;
        }
    }
}
