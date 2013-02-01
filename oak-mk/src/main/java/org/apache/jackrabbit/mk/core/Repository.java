/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.core;

import java.io.Closeable;
import java.io.File;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.FileBlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.model.CommitBuilder;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.persistence.H2Persistence;
import org.apache.jackrabbit.mk.persistence.InMemPersistence;
import org.apache.jackrabbit.mk.store.DefaultRevisionStore;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.store.RevisionStore;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 *
 */
public class Repository {

    private final File homeDir;
    private boolean initialized;
    private RevisionStore rs;
    private BlobStore bs;
    private boolean blobStoreNeedsClose;

    public Repository(String homeDir) throws Exception {
        File home = new File(homeDir == null ? "." : homeDir, ".mk");
        this.homeDir = home.getCanonicalFile();
    }
    
    /**
     * Alternate constructor, used for testing.
     * 
     * @param rs revision store
     * @param bs blob store
     */
    public Repository(RevisionStore rs, BlobStore bs) {
        this.homeDir = null;
        this.rs = rs;
        this.bs = bs;

        initialized = true;
    }

    /**
     * Argument-less constructor, used for in-memory kernel.
     */
    protected Repository() {
        this.homeDir = null;

        DefaultRevisionStore rs =
                new DefaultRevisionStore(new InMemPersistence(), null);

        try {
            rs.initialize();
        } catch (Exception e) {
            /* Not plausible for in-memory operation */
            throw new InternalError("Unable to initialize in-memory store");
        }
        this.rs = rs;
        this.bs = new MemoryBlobStore();
        
        initialized = true;
    }
    
    public void init() throws Exception {
        if (initialized) {
            return;
        }

        H2Persistence pm = new H2Persistence();
        pm.initialize(homeDir);
        
        DefaultRevisionStore rs = new DefaultRevisionStore(pm);
        rs.initialize();
        
        this.rs = rs;
        
        if (pm instanceof BlobStore) {
            bs = (BlobStore) pm;
        } else {
            bs = new FileBlobStore(new File(homeDir, "blobs").getCanonicalPath());
            blobStoreNeedsClose = true;
        }
        
        initialized = true;
    }

    public void shutDown() throws Exception {
        if (!initialized) {
            return;
        }
        if (blobStoreNeedsClose && bs instanceof Closeable) {
            IOUtils.closeQuietly((Closeable) bs);
        }
        if (rs instanceof Closeable) {
            IOUtils.closeQuietly((Closeable) rs);
        }
        initialized = false;
    }

    public RevisionStore getRevisionStore() {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }

        return rs;
    }
    
    public BlobStore getBlobStore() {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }

        return bs;
    }

    public Id getHeadRevision() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        return rs.getHeadCommitId();
    }

    public Id getBaseRevision(Id branchRevision) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        StoredCommit commit = rs.getCommit(branchRevision);
        return commit == null ? null : commit.getBranchRootId();
    }

    public StoredCommit getHeadCommit() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        return rs.getHeadCommit();
    }

    public StoredCommit getCommit(Id id) throws NotFoundException, Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        return rs.getCommit(id);
    }

    public NodeState getNodeState(Id revId, String path) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        } else if (!PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("illegal path");
        }

        NodeState node = rs.getNodeState(rs.getRootNode(revId));
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
            if (node == null) {
                break;
            }
        }
        return node;
    }

    public boolean nodeExists(Id revId, String path) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        } else if (!PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("illegal path");
        }

        NodeState node = rs.getNodeState(rs.getRootNode(revId));
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
            if (node == null) {
                return false;
            }
        }
        return true;
    }

    public CommitBuilder getCommitBuilder(Id revId, String msg) throws Exception {
        return new CommitBuilder(revId, msg, rs);
    }

}
