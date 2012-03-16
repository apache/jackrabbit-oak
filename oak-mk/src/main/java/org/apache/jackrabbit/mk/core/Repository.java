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

import org.apache.jackrabbit.mk.model.CommitBuilder;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.store.DefaultRevisionStore;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.store.RevisionStore;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mk.util.PathUtils;

/**
 *
 */
public class Repository {

    private final String homeDir;
    private boolean initialized;
    private RevisionStore rs;

    public Repository(String homeDir) throws Exception {
        File home = new File(homeDir == null ? "." : homeDir, ".mk");
        this.homeDir = home.getCanonicalPath();
    }
    
    /**
     * Alternate constructor, used for testing.
     * 
     * @param rs revision store, already initialized
     */
    public Repository(RevisionStore rs) {
        this.homeDir = null;
        this.rs = rs;
        
        initialized = true;
    }
    
    public void init() throws Exception {
        if (initialized) {
            return;
        }
        DefaultRevisionStore rs = new DefaultRevisionStore();
        rs.initialize(new File(homeDir));
        this.rs = rs;

        initialized = true;
    }

    public void shutDown() throws Exception {
        if (!initialized) {
            return;
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

    public Id getHeadRevision() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        return rs.getHeadCommitId();
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

    public NodeState getNodeState(Id revId, String path)
            throws NotFoundException, Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        } else if (!PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("illegal path");
        }

        NodeState node = rs.getNodeState(rs.getRootNode(revId));
        for (String name : PathUtils.split(path)) {
            node = node.getChildNode(name);
            if (node == null) {
                throw new NotFoundException(
                        "Path " + path + " not found in revision " + revId);
            }
        }
        return node;
    }

    public boolean nodeExists(Id revId, String path) {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        } else if (!PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("illegal path");
        }

        try {
            NodeState node = rs.getNodeState(rs.getRootNode(revId));
            for (String name : PathUtils.split(path)) {
                node = node.getChildNode(name);
                if (node == null) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to check for existence of path "
                    + path + " in revision " + revId, e);
        }
    }

    public CommitBuilder getCommitBuilder(Id revId, String msg) throws Exception {
        return new CommitBuilder(revId, msg, rs);
    }

}
