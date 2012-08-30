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
package org.apache.jackrabbit.oak.core;

import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.ChangeExtractor;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ConflictHandler;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.security.authorization.AccessControlContextImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlContext;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RootImpl implements Root {
    static final Logger log = LoggerFactory.getLogger(RootImpl.class);

    /**
     * Number of {@link #purge()} calls for which changes are kept in memory.
     */
    private static final int PURGE_LIMIT = 100;

    /** The underlying store to which this root belongs */
    private final NodeStore store;

    private final Subject subject;

    /** Current branch this root operates on */
    private NodeStoreBranch branch;

    /** Current root {@code Tree} */
    private TreeImpl rootTree;

    /**
     * Number of {@link #purge()} occurred so since the lase
     * purge.
     */
    private int modCount;

    /**
     * Listeners which needs to be notified as soon as {@link #purgePendingChanges()}
     * is called. Listeners are removed from this list after being called. If further
     * notifications are required, they need to explicitly re-register.
     *
     * The {@link TreeImpl} instances us this mechanism to dispose of its associated
     * {@link NodeBuilder} on purge. Keeping a reference on those {@code TreeImpl}
     * instances {@code NodeBuilder} (i.e. those which are modified) prevents them
     * from being prematurely garbage collected.
     */
    private List<PurgeListener> purgePurgeListeners = new ArrayList<PurgeListener>();

    /**
     * Purge listener.
     * @see #purgePurgeListeners
     */
    public interface PurgeListener {
        void purged();
    }

    /**
     * New instance bases on a given {@link NodeStore} and a workspace
     * @param store  node store
     * @param workspaceName  name of the workspace
     * @param accessControlContext
     */
    @SuppressWarnings("UnusedParameters")
    public RootImpl(NodeStore store, String workspaceName, Subject subject) {
        this.store = store;
        this.subject = subject;
        refresh();
    }

    //---------------------------------------------------------------< Root >---
    @Override
    public boolean move(String sourcePath, String destPath) {
        TreeImpl source = rootTree.getTree(sourcePath);
        if (source == null) {
            return false;
        }
        TreeImpl destParent = rootTree.getTree(getParentPath(destPath));
        if (destParent == null) {
            return false;
        }

        String destName = getName(destPath);
        if (destParent.hasChild(destName)) {
            return false;
        }

        purgePendingChanges();
        source.moveTo(destParent, destName);
        return branch.move(sourcePath, destPath);
    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        purgePendingChanges();
        return branch.copy(sourcePath, destPath);
    }

    @Override
    public TreeImpl getTree(String path) {
        return rootTree.getTree(path);
    }

    @Override
    public TreeLocation getLocation(String path) {
        assert path.startsWith("/");
        return rootTree.getLocation().getChild(path.substring(1));
    }

    @Override
    public void rebase(ConflictHandler conflictHandler) {
        if (!store.getRoot().equals(rootTree.getBaseState())) {
            purgePendingChanges();
            NodeState base = getBaseState();
            NodeState head = rootTree.getNodeState();
            refresh();
            MergingNodeStateDiff.merge(base, head, rootTree, conflictHandler);
        }
    }

    @Override
    public final void refresh() {
        branch = store.branch();
        rootTree = TreeImpl.createRoot(this);
    }

    @Override
    public void commit(ConflictHandler conflictHandler) throws CommitFailedException {
        rebase(conflictHandler);
        purgePendingChanges();
        CommitFailedException exception = Subject.doAs(
                subject, new PrivilegedAction<CommitFailedException>() {
                    @Override
                    public CommitFailedException run() {
                        try {
                            branch.merge();
                            return null;
                        } catch (CommitFailedException e) {
                            return e;
                        }
                    }
                });
        if (exception != null) {
            throw exception;
        }
        refresh();
    }

    @Override
    public boolean hasPendingChanges() {
        return !getBaseState().equals(rootTree.getNodeState());
    }

    @Override
    @Nonnull
    public ChangeExtractor getChangeExtractor() {
        return new ChangeExtractor() {
            private NodeState baseLine = store.getRoot();

            @Override
            public void getChanges(NodeStateDiff diff) {
                NodeState head = store.getRoot();
                head.compareAgainstBaseState(baseLine, diff);
                baseLine = head;
            }
        };
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Returns the node state from which the current branch was created.
     * @return base node state
     */
    @Nonnull
    NodeState getBaseState() {
        return branch.getBase();
    }

    NodeBuilder createRootBuilder() {
        return store.getBuilder(branch.getRoot());
    }

    /**
     * Add a {@code PurgeListener} to this instance. Listeners are automatically
     * unregistered after having been called. If further notifications are required,
     * they need to explicitly re-register.
     * @param purgeListener  listener
     */
    void addListener(PurgeListener purgeListener) {
        purgePurgeListeners.add(purgeListener);
    }

    // TODO better way to determine purge limit. See OAK-175
    void purge() {
        if (++modCount > PURGE_LIMIT) {
            modCount = 0;
            purgePendingChanges();
        }
    }

    CompiledPermissions getPermissions() {
        AccessControlContext context = new AccessControlContextImpl();
        context.initialize(subject.getPrincipals());
        return context.getPermissions();
    }

    //------------------------------------------------------------< private >---

    /**
     * Purge all pending changes to the underlying {@link NodeStoreBranch}.
     * All registered {@link PurgeListener}s are notified.
     */
    private void purgePendingChanges() {
        branch.setRoot(rootTree.getNodeState());
        notifyListeners();
    }

    private void notifyListeners() {
        List<PurgeListener> purgeListeners = this.purgePurgeListeners;
        this.purgePurgeListeners = new ArrayList<PurgeListener>();

        for (PurgeListener purgeListener : purgeListeners) {
            purgeListener.purged();
        }
    }

}
