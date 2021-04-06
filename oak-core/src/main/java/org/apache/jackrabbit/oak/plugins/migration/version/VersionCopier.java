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

import java.util.Calendar;
import java.util.Iterator;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.plugins.migration.DescendantsIterator;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier;
import org.apache.jackrabbit.oak.plugins.version.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;

import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getRelativeVersionHistoryPath;
import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getVersionHistoryLastModified;
import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getVersionHistoryNodeState;

/**
 * This class allows to copy the version history, optionally filtering it with a
 * given date.
 */
public class VersionCopier {

    private final NodeState sourceVersionStorage;

    private final NodeBuilder targetVersionStorage;

    private final Supplier<Boolean> frozenNodeIsReferenceable;

    public VersionCopier(NodeBuilder targetRoot, NodeState sourceVersionStorage, NodeBuilder targetVersionStorage) {
        this.sourceVersionStorage = sourceVersionStorage;
        this.targetVersionStorage = targetVersionStorage;
        this.frozenNodeIsReferenceable = new IsFrozenNodeReferenceable(targetRoot.getNodeState());
    }

    public static void copyVersionStorage(NodeBuilder targetRoot, NodeState sourceVersionStorage, NodeBuilder targetVersionStorage, VersionCopyConfiguration config) {
        final Iterator<NodeState> versionStorageIterator = new DescendantsIterator(sourceVersionStorage, 3);
        final VersionCopier versionCopier = new VersionCopier(targetRoot, sourceVersionStorage, targetVersionStorage);

        while (versionStorageIterator.hasNext()) {
            final NodeState versionHistoryBucket = versionStorageIterator.next();
            for (String versionHistory : versionHistoryBucket.getChildNodeNames()) {
                versionCopier.copyVersionHistory(versionHistory, config.getOrphanedMinDate());
            }
        }
    }

    /**
     * Copy history filtering versions using passed date and returns {@code
     * true} if the history has been copied.
     * 
     * @param versionableUuid
     *            Name of the version history node
     * @param minDate
     *            Only versions older than this date will be copied
     * @return {@code true} if at least one version has been copied
     */
    public boolean copyVersionHistory(String versionableUuid, Calendar minDate) {
        final String versionHistoryPath = getRelativeVersionHistoryPath(versionableUuid);
        final NodeState sourceVersionHistory = getVersionHistoryNodeState(sourceVersionStorage, versionableUuid);
        final Calendar lastModified = getVersionHistoryLastModified(sourceVersionHistory);

        if (sourceVersionHistory.exists() && (lastModified.after(minDate) || minDate.getTimeInMillis() == 0)) {
            NodeStateCopier.builder()
                    .include(versionHistoryPath)
                    .merge(VERSION_STORE_PATH)
                    .withReferenceableFrozenNodes(frozenNodeIsReferenceable.get())
                    .copy(sourceVersionStorage, targetVersionStorage);
            return true;
        }
        return false;
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
