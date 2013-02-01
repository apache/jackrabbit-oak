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
package org.apache.jackrabbit.oak.plugins.index.old.mk.wrapper;

import java.io.InputStream;
import java.util.HashSet;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;

/**
 * A MicroKernel wrapper that provides limited support for branch and merge even
 * if the underlying implementation does not support it.
 */
public class BranchMergeMicroKernel implements MicroKernel {

    private static final String TRUNK = "trunk";

    private final MicroKernel base;
    private final HashSet<String> knownBranches = new HashSet<String>();
    private int nextBranch;
    private String trunkHeadRevision;
    private String busyBranch;

    public BranchMergeMicroKernel(MicroKernel base) {
        this.base = base;
    }

    @Override
    public synchronized String branch(String trunkRevisionId) {
        if (trunkRevisionId == null) {
            trunkRevisionId = getHeadRevision();
        }
        String head = base.getHeadRevision();
        String branchId = getBranchId(trunkRevisionId);
        if (!TRUNK.equals(branchId)) {
            throw new MicroKernelException("Cannot branch off a branch: "
                    + trunkRevisionId);
        }
        trunkHeadRevision = head;
        String branch = "b" + nextBranch++;
        knownBranches.add(branch);
        String branchRev = branch + "-" + getHeadRevision();
        busyBranch = null;
        return branchRev;
    }

    private synchronized boolean isKnownBranch(String branchId) {
        if (TRUNK.equals(branchId)) {
            return false;
        }
        return knownBranches.contains(branchId);
    }

    private static String getBranchId(String revisionId) {
        if (revisionId == null) {
            return TRUNK;
        }
        int idx = revisionId.indexOf('-');
        if (idx <= 0) {
            return TRUNK;
        }
        return revisionId.substring(0, idx);
    }

    private static String getRevisionId(String branchId, String revisionId) {
        if (TRUNK.equals(branchId)) {
            return revisionId;
        }
        return revisionId.substring(branchId.length() + 1);
    }

    private static String getCombinedRevisionId(String branchId,
            String revisionId) {
        if (TRUNK.equals(branchId)) {
            return revisionId;
        }
        return branchId + "-" + revisionId;
    }

    @Override
    public synchronized String commit(String path, String jsonDiff,
            String revisionId, String message) {
        if (revisionId == null) {
            revisionId = getHeadRevision();
        }
        String branchId = getBranchId(revisionId);
        // if another branch is active, wait until it's changes are merged
        while (true) {
            if (!TRUNK.equals(branchId) && !isKnownBranch(branchId)) {
                throw new MicroKernelException("Unknown branch: " + revisionId);
            }
            if (busyBranch == null || branchId.equals(busyBranch)) {
                break;
            }
            try {
                wait(1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        busyBranch = branchId;
        String rev = getRevisionId(branchId, revisionId);
        String rev2 = base.commit(path, jsonDiff, rev, message);
        return getCombinedRevisionId(branchId, rev2);
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path,
            int depth) {
        String fromBranch = getBranchId(fromRevisionId);
        String toBranch = getBranchId(toRevisionId);
        String from = getRevisionId(fromBranch, fromRevisionId);
        String to = getRevisionId(toBranch, toRevisionId);
        return base.diff(from, to, path, depth);
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) {
        String branch = getBranchId(revisionId);
        String rev = getRevisionId(branch, revisionId);
        return base.getChildNodeCount(path, rev);
    }

    @Override
    public String getHeadRevision() {
        if (trunkHeadRevision != null) {
            return trunkHeadRevision;
        }
        return base.getHeadRevision();
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) {
        String fromBranch = getBranchId(fromRevisionId);
        String toBranch = getBranchId(toRevisionId);
        if (!fromBranch.equals(TRUNK) || !toBranch.equals(TRUNK)) {
            throw new MicroKernelException(
                    "This operation is not supported on branches: "
                            + fromRevisionId + " - " + toRevisionId);
        }
        return base.getJournal(fromRevisionId, toRevisionId, path);
    }

    @Override
    public long getLength(String blobId) {
        return base.getLength(blobId);
    }

    @Override
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter) {
        String branch = getBranchId(revisionId);
        String rev = getRevisionId(branch, revisionId);
        return base.getNodes(path, rev, depth, offset, maxChildNodes, filter);
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) {
        return base.getRevisionHistory(since, maxEntries, path);
    }

    @Override
    public String merge(String branchRevisionId, String message) {
        String branch = getBranchId(branchRevisionId);
        if (TRUNK.equals(branch)) {
            throw new MicroKernelException("Can not merge the trunk");
        }
        knownBranches.remove(branch);
        busyBranch = null;
        trunkHeadRevision = null;
        return getHeadRevision();
    }

    @Nonnull
    @Override
    public String rebase(@Nonnull String branchRevisionId, String newBaseRevisionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeExists(String path, String revisionId) {
        String branch = getBranchId(revisionId);
        String rev = getRevisionId(branch, revisionId);
        return base.nodeExists(path, rev);
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) {
        return base.read(blobId, pos, buff, off, length);
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout)
            throws InterruptedException {
        String branch = getBranchId(oldHeadRevisionId);
        String rev = getRevisionId(branch, oldHeadRevisionId);
        return base.waitForCommit(rev, timeout);
    }

    @Override
    public String write(InputStream in) {
        return base.write(in);
    }

    @Override
    public String toString() {
        return getClass().getName() + ":" + base;
    }

}
