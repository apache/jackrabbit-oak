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
package org.apache.jackrabbit.oak.upgrade.version;

import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.JcrConstants.NT_VERSION;

import java.util.Calendar;
import java.util.Iterator;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.upgrade.DescendantsIterator;
import org.apache.jackrabbit.oak.upgrade.nodestate.NodeStateCopier;
import org.apache.jackrabbit.util.ISO8601;

import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.VERSION_STORE_PATH;

import static org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil.getVersionHistoryPath;
import static org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil.getVersionHistoryNodeState;

/**
 * This class allows to copy the version history, optionally filtering it with a
 * given date.
 */
public class VersionCopier {

    private final TypePredicate isVersion;

    private final NodeState sourceRoot;

    private final NodeBuilder targetRoot;

    public VersionCopier(NodeState sourceRoot, NodeBuilder targetRoot) {
        this.isVersion = new TypePredicate(targetRoot.getNodeState(), NT_VERSION);
        this.sourceRoot = sourceRoot;
        this.targetRoot = targetRoot;
    }

    public static void copyVersionStorage(NodeState sourceRoot, NodeBuilder targetRoot, VersionCopyConfiguration config) {
        final NodeState versionStorage = sourceRoot.getChildNode(JCR_SYSTEM).getChildNode(JCR_VERSIONSTORAGE);
        final Iterator<NodeState> versionStorageIterator = new DescendantsIterator(versionStorage, 3);
        final VersionCopier versionCopier = new VersionCopier(sourceRoot, targetRoot);

        while (versionStorageIterator.hasNext()) {
            final NodeState versionHistoryBucket = versionStorageIterator.next();
            for (String versionHistory : versionHistoryBucket.getChildNodeNames()) {
                versionCopier.copyVersionHistory(versionHistory, config.getOrphanedMinDate());
            }
        }
    }

    /**
     * Copy history filtering versions using passed date and returns @{code
     * true} if at least one version has been copied.
     * 
     * @param versionableUuid
     *            Name of the version history node
     * @param minDate
     *            Only versions older than this date will be copied
     * @return {@code true} if at least one version has been copied
     */
    public boolean copyVersionHistory(String versionableUuid, Calendar minDate) {
        final String versionHistoryPath = getVersionHistoryPath(versionableUuid);
        final NodeState versionHistory = getVersionHistoryNodeState(sourceRoot, versionableUuid);
        final Calendar lastModified = getVersionHistoryLastModified(versionHistory);

        if (lastModified.after(minDate) || minDate.getTimeInMillis() == 0) {
            NodeStateCopier.builder()
                    .include(versionHistoryPath)
                    .merge(VERSION_STORE_PATH)
                    .copy(sourceRoot, targetRoot);
            return true;
        }
        return false;
    }

    private Calendar getVersionHistoryLastModified(final NodeState versionHistory) {
        Calendar youngest = Calendar.getInstance();
        youngest.setTimeInMillis(0);
        for (final ChildNodeEntry entry : versionHistory.getChildNodeEntries()) {
            final NodeState version = entry.getNodeState();
            if (!isVersion.apply(version)) {
                continue;
            }
            if (version.hasProperty(JCR_CREATED)) {
                final Calendar created = ISO8601.parse(version.getProperty(JCR_CREATED).getValue(Type.DATE));
                if (created.after(youngest)) {
                    youngest = created;
                }
            }
        }
        return youngest;
    }
}
