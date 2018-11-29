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

package org.apache.jackrabbit.oak.segment.file.tooling;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

public class ConsistencyChecker {

    private static NodeState getDescendantOrNull(NodeState root, String path) {
        NodeState descendant = NodeStateUtils.getNode(root, path);
        if (descendant.exists()) {
            return descendant;
        }
        return null;
    }

    private static class PathToCheck {

        final String path;

        JournalEntry journalEntry;

        Set<String> corruptPaths = new LinkedHashSet<>();

        PathToCheck(String path) {
            this.path = path;
        }

    }

    protected void onCheckRevision(String revision) {
        // Do nothing.
    }

    protected void onCheckHead() {
        // Do nothing.
    }

    protected  void onCheckChekpoints() {
        // Do nothing.
    }

    protected  void onCheckCheckpoint(String checkpoint) {
        // Do nothing.
    }

    protected void onCheckpointNotFoundInRevision(String checkpoint) {
        // Do nothing.
    }

    protected void onCheckRevisionError(String revision, Exception e) {
        // Do nothing.
    }

    protected void onConsistentPath(String path) {
        // Do nothing.
    }

    protected void onPathNotFound(String path) {
        // Do nothing.
    }

    protected void onCheckTree(String path) {
        // Do nothing.
    }

    protected void onCheckTreeEnd() {
        // Do nothing.
    }

    protected void onCheckNode(String path) {
        // Do nothing.
    }

    protected void onCheckProperty() {
        // Do nothing.
    }

    protected void onCheckPropertyEnd(String path, PropertyState property) {
        // Do nothing.
    }

    protected void onCheckNodeError(String path, Exception e) {
        // Do nothing.
    }

    protected void onCheckTreeError(String path, Exception e) {
        // Do nothing.
    }

    public static class Revision {

        private final String revision;

        private final long timestamp;

        private Revision(String revision, long timestamp) {
            this.revision = revision;
            this.timestamp = timestamp;
        }

        public String getRevision() {
            return revision;
        }

        public long getTimestamp() {
            return timestamp;
        }

    }

    public static class ConsistencyCheckResult {

        private ConsistencyCheckResult() {
            // Prevent external instantiation.
        }

        private final Map<String, Revision> headRevisions = new HashMap<>();

        private final Map<String, Map<String, Revision>> checkpointRevisions = new HashMap<>();

        private Revision overallRevision;

        private int checkedRevisionsCount;

        public int getCheckedRevisionsCount() {
            return checkedRevisionsCount;
        }

        public Revision getOverallRevision() {
            return overallRevision;
        }

        public Map<String, Revision> getHeadRevisions() {
            return headRevisions;
        }

        public Map<String, Map<String, Revision>> getCheckpointRevisions() {
            return checkpointRevisions;
        }

    }

    private boolean isPathInvalid(NodeState root, String path, boolean binaries) {
        NodeState node = getDescendantOrNull(root, path);

        if (node == null) {
            onPathNotFound(path);
            return true;
        }

        return checkNode(node, path, binaries) != null;
    }

    private String findFirstCorruptedPathInSet(NodeState root, Set<String> corruptedPaths, boolean binaries) {
        for (String corruptedPath : corruptedPaths) {
            if (isPathInvalid(root, corruptedPath, binaries)) {
                return corruptedPath;
            }
        }
        return null;
    }

    private String findFirstCorruptedPathInTree(NodeState root, String path, boolean binaries) {
        NodeState node = getDescendantOrNull(root, path);

        if (node == null) {
            onPathNotFound(path);
            return path;
        }

        return checkNodeAndDescendants(node, path, binaries);
    }

    private String checkTreeConsistency(NodeState root, String path, Set<String> corruptedPaths, boolean binaries) {
        String corruptedPath = findFirstCorruptedPathInSet(root, corruptedPaths, binaries);

        if (corruptedPath != null) {
            return corruptedPath;
        }

        onCheckTree(path);
        corruptedPath = findFirstCorruptedPathInTree(root, path, binaries);
        onCheckTreeEnd();
        return corruptedPath;
    }

    private boolean checkPathConsistency(NodeState root, PathToCheck ptc, JournalEntry entry, boolean binaries) {
        if (ptc.journalEntry != null) {
            return true;
        }

        String corruptPath = checkTreeConsistency(root, ptc.path, ptc.corruptPaths, binaries);

        if (corruptPath != null) {
            ptc.corruptPaths.add(corruptPath);
            return false;
        }

        onConsistentPath(ptc.path);
        ptc.journalEntry = entry;
        return true;
    }

    private boolean checkAllPathsConsistency(NodeState root, List<PathToCheck> paths, JournalEntry entry, boolean binaries) {
        boolean result = true;

        for (PathToCheck ptc : paths) {
            if (!checkPathConsistency(root, ptc, entry, binaries)) {
                result = false;
            }
        }

        return result;
    }

    private boolean checkHeadConsistency(SegmentNodeStore store, List<PathToCheck> paths, JournalEntry entry, boolean binaries) {
        boolean allConsistent = paths.stream().allMatch(p -> p.journalEntry != null);

        if (allConsistent) {
            return true;
        }

        onCheckHead();
        return checkAllPathsConsistency(store.getRoot(), paths, entry, binaries);
    }

    private boolean checkCheckpointConsistency(SegmentNodeStore store, String checkpoint, List<PathToCheck> paths, JournalEntry entry, boolean binaries) {
        boolean allConsistent = paths.stream().allMatch(p -> p.journalEntry != null);

        if (allConsistent) {
            return true;
        }

        onCheckCheckpoint(checkpoint);

        NodeState root = store.retrieve(checkpoint);

        if (root == null) {
            onCheckpointNotFoundInRevision(checkpoint);
            return false;
        }

        return checkAllPathsConsistency(root, paths, entry, binaries);
    }

    private boolean checkCheckpointsConsistency(SegmentNodeStore store, Map<String, List<PathToCheck>> paths, JournalEntry entry, boolean binaries) {
        boolean result = true;
        for (Entry<String, List<PathToCheck>> e : paths.entrySet()) {
            if (!checkCheckpointConsistency(store, e.getKey(), e.getValue(), entry, binaries)) {
                result = false;
            }
        }
        return result;
    }

    private boolean allPathsConsistent(List<PathToCheck> headPaths, Map<String, List<PathToCheck>> checkpointPaths) {
        for (PathToCheck path : headPaths) {
            if (path.journalEntry == null) {
                return false;
            }
        }

        for (Entry<String, List<PathToCheck>> e : checkpointPaths.entrySet()) {
            for (PathToCheck path : e.getValue()) {
                if (path.journalEntry == null) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean shouldCheckCheckpointsConsistency(Map<String, List<PathToCheck>> paths) {
        return paths
            .values()
            .stream()
            .flatMap(List::stream)
            .anyMatch(p -> p.journalEntry == null);
    }

    /**
     * Check the consistency of a given subtree and returns the first
     * inconsistent path. If provided, this method probes a set of inconsistent
     * paths before performing a full traversal of the subtree.
     *
     * @param root           The root node of the subtree.
     * @param corruptedPaths A set of possibly inconsistent paths.
     * @param binaries       Whether to check binaries for consistency.
     * @return The first inconsistent path or {@code null}. The path might be
     * either one of the provided inconsistent paths or a new one discovered
     * during a full traversal of the tree.
     */
    public String checkTreeConsistency(NodeState root, Set<String> corruptedPaths, boolean binaries) {
        return checkTreeConsistency(root, "/", corruptedPaths, binaries);
    }

    public final ConsistencyCheckResult checkConsistency(
        ReadOnlyFileStore store,
        Iterator<JournalEntry> journal,
        boolean head,
        Set<String> checkpoints,
        Set<String> paths,
        boolean binaries
    ) {
        List<PathToCheck> headPaths = new ArrayList<>();
        Map<String, List<PathToCheck>> checkpointPaths = new HashMap<>();

        int revisionCount = 0;

        for (String path : paths) {
            if (head) {
                headPaths.add(new PathToCheck(path));
            }

            for (String checkpoint : checkpoints) {
                checkpointPaths
                    .computeIfAbsent(checkpoint, k -> new ArrayList<>())
                    .add(new PathToCheck(path));
            }
        }

        JournalEntry lastValidJournalEntry = null;

        SegmentNodeStore sns = SegmentNodeStoreBuilders.builder(store).build();

        while (journal.hasNext()) {
            JournalEntry journalEntry = journal.next();
            String revision = journalEntry.getRevision();

            try {
                revisionCount++;
                store.setRevision(revision);
                onCheckRevision(revision);

                // Check the consistency of both the head and the checkpoints.
                // If both are consistent, the current journal entry is the
                // overall valid entry.

                boolean overall = checkHeadConsistency(sns, headPaths, journalEntry, binaries);

                if (shouldCheckCheckpointsConsistency(checkpointPaths)) {
                    onCheckChekpoints();
                    overall = overall && checkCheckpointsConsistency(sns, checkpointPaths, journalEntry, binaries);
                }

                if (overall) {
                    lastValidJournalEntry = journalEntry;
                }

                // If every PathToCheck is assigned to a JournalEntry, stop
                // looping through the journal.

                if (allPathsConsistent(headPaths, checkpointPaths)) {
                    break;
                }
            } catch (IllegalArgumentException | SegmentNotFoundException e) {
                onCheckRevisionError(revision, e);
            }
        }

        ConsistencyCheckResult result = new ConsistencyCheckResult();

        result.checkedRevisionsCount = revisionCount;
        result.overallRevision = newRevisionOrNull(lastValidJournalEntry);

        for (PathToCheck path : headPaths) {
            result.headRevisions.put(path.path, newRevisionOrNull(path.journalEntry));
        }

        for (String checkpoint : checkpoints) {
            for (PathToCheck path : checkpointPaths.get(checkpoint)) {
                result.checkpointRevisions
                    .computeIfAbsent(checkpoint, s -> new HashMap<>())
                    .put(path.path, newRevisionOrNull(path.journalEntry));
            }
        }

        return result;
    }

    private static Revision newRevisionOrNull(JournalEntry entry) {
        if (entry == null) {
            return null;
        }
        return new Revision(entry.getRevision(), entry.getTimestamp());
    }

    /**
     * Checks the consistency of a node and its properties at the given path.
     *
     * @param node          node to be checked
     * @param path          path of the node
     * @param checkBinaries if {@code true} full content of binary properties
     *                      will be scanned
     * @return {@code null}, if the node is consistent, or the path of the first
     * inconsistency otherwise.
     */
    private String checkNode(NodeState node, String path, boolean checkBinaries) {
        try {
            onCheckNode(path);
            for (PropertyState propertyState : node.getProperties()) {
                Type<?> type = propertyState.getType();
                boolean checked = false;

                if (type == BINARY) {
                    checked = traverse(propertyState.getValue(BINARY), checkBinaries);
                } else if (type == BINARIES) {
                    for (Blob blob : propertyState.getValue(BINARIES)) {
                        checked = checked | traverse(blob, checkBinaries);
                    }
                } else {
                    propertyState.getValue(type);
                    onCheckProperty();
                    checked = true;
                }

                if (checked) {
                    onCheckPropertyEnd(path, propertyState);
                }
            }

            return null;
        } catch (RuntimeException | IOException e) {
            onCheckNodeError(path, e);
            return path;
        }
    }

    /**
     * Recursively checks the consistency of a node and its descendants at the
     * given path.
     *
     * @param node          node to be checked
     * @param path          path of the node
     * @param checkBinaries if {@code true} full content of binary properties
     *                      will be scanned
     * @return {@code null}, if the node is consistent, or the path of the first
     * inconsistency otherwise.
     */
    private String checkNodeAndDescendants(NodeState node, String path, boolean checkBinaries) {
        String result = checkNode(node, path, checkBinaries);
        if (result != null) {
            return result;
        }

        try {
            for (ChildNodeEntry cne : node.getChildNodeEntries()) {
                String childName = cne.getName();
                NodeState child = cne.getNodeState();
                result = checkNodeAndDescendants(child, concat(path, childName), checkBinaries);
                if (result != null) {
                    return result;
                }
            }

            return null;
        } catch (RuntimeException e) {
            onCheckTreeError(path, e);
            return path;
        }
    }

    private boolean traverse(Blob blob, boolean checkBinaries) throws IOException {
        if (checkBinaries && !isExternal(blob)) {
            try (InputStream s = blob.getNewStream()) {
                byte[] buffer = new byte[8192];
                int l = s.read(buffer, 0, buffer.length);
                while (l >= 0) {
                    l = s.read(buffer, 0, buffer.length);
                }
            }
            onCheckProperty();
            return true;
        }

        return false;
    }

    private static boolean isExternal(Blob b) {
        if (b instanceof SegmentBlob) {
            return ((SegmentBlob) b).isExternal();
        }
        return false;
    }

}


