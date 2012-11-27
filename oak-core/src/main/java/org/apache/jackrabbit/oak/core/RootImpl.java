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

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.BlobFactory;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.plugins.commit.DefaultConflictHandler;
import org.apache.jackrabbit.oak.query.SessionQueryEngineImpl;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.observation.ChangeExtractor;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

public class RootImpl implements Root {

    /**
     * Number of {@link #updated} calls for which changes are kept in memory.
     */
    private static final int PURGE_LIMIT = 100;

    /** The underlying store to which this root belongs */
    private final NodeStore store;

    private final Subject subject;

    /**
     * The access control context provider.
     */
    private final AccessControlConfiguration accConfiguration;

    /** Current branch this root operates on */
    private NodeStoreBranch branch;

    /** Current root {@code Tree} */
    private TreeImpl rootTree;

    /**
     * Number of {@link #updated} occurred so since the lase
     * purge.
     */
    private int modCount;

    private volatile ConflictHandler conflictHandler = DefaultConflictHandler.OURS;

    private final QueryIndexProvider indexProvider;

    /**
     * New instance bases on a given {@link NodeStore} and a workspace
     *
     * @param store         node store
     * @param workspaceName name of the workspace
     * @param subject       the subject.
     * @param accConfiguration   the access control context provider.
     * @param indexProvider the query index provider.
     */
    @SuppressWarnings("UnusedParameters")
    public RootImpl(NodeStore store,
                    String workspaceName,
                    Subject subject,
                    AccessControlConfiguration accConfiguration,
                    QueryIndexProvider indexProvider) {
        this.store = checkNotNull(store);
        this.subject = checkNotNull(subject);
        this.accConfiguration = checkNotNull(accConfiguration);
        this.indexProvider = indexProvider;
        refresh();
    }

    // TODO: review if this constructor really makes sense and cannot be replaced.
    public RootImpl(NodeStore store) {
        this.store = checkNotNull(store);
        this.subject = new Subject(true, Collections.singleton(SystemPrincipal.INSTANCE), Collections.<Object>emptySet(), Collections.<Object>emptySet());
        this.accConfiguration = new OpenAccessControlConfiguration();
        this.indexProvider = new CompositeQueryIndexProvider();
        refresh();
    }

    /**
     * Oak level variant of {@link org.apache.jackrabbit.oak.api.ContentSession#getLatestRoot()}
     * to be used when no {@code ContentSession} is available.
     *
     * @return A new Root instance.
     * @see org.apache.jackrabbit.oak.api.ContentSession#getLatestRoot()
     */
    public Root getLatest() {
        checkLive();
        RootImpl root = new RootImpl(store, null, subject, accConfiguration, indexProvider) {
            protected void checkLive() {
                RootImpl.this.checkLive();
            }
        };
        if (conflictHandler != null) {
            root.setConflictHandler(conflictHandler);
        }
        return root;
    }

    void setConflictHandler(ConflictHandler conflictHandler) {
        this.conflictHandler = conflictHandler;
    }

    /**
     * Called whenever a method on this instance or on any {@code Tree} instance
     * obtained from this {@code Root} is called. This default implementation
     * does nothing. Sub classes may override this method and throw an exception
     * indicating that this {@code Root} instance is not live anymore (e.g. because
     * the session has been logged out already).
     */
    protected void checkLive() {

    }

    //---------------------------------------------------------------< Root >---
    @Override
    public boolean move(String sourcePath, String destPath) {
        checkLive();
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
        boolean success = branch.move(sourcePath, destPath);
        reset();
        if (success) {
            getTree(getParentPath(sourcePath)).updateChildOrder();
            getTree(getParentPath(destPath)).updateChildOrder();
        }
        return success;
    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        checkLive();
        purgePendingChanges();
        boolean success = branch.copy(sourcePath, destPath);
        reset();
        if (success) {
            getTree(getParentPath(destPath)).updateChildOrder();
        }
        return success;
    }

    @Override
    public TreeImpl getTree(String path) {
        checkLive();
        return rootTree.getTree(path);
    }

    @Override
    public TreeLocation getLocation(String path) {
        checkLive();
        checkArgument(path.startsWith("/"));
        return rootTree.getLocation().getChild(path.substring(1));
    }

    @Override
    public void rebase() {
        checkLive();
        if (!store.getRoot().equals(rootTree.getBaseState())) {
            purgePendingChanges();
            NodeState base = getBaseState();
            NodeState head = rootTree.getNodeState();
            refresh();
            MergingNodeStateDiff.merge(base, head, rootTree.getNodeBuilder(), conflictHandler);
        }
    }

    @Override
    public final void refresh() {
        checkLive();
        branch = store.branch();
        rootTree = TreeImpl.createRoot(this);
    }

    @Override
    public void commit() throws CommitFailedException {
        checkLive();
        rebase();
        purgePendingChanges();
        CommitFailedException exception = Subject.doAs(
                getCombinedSubject(), new PrivilegedAction<CommitFailedException>() {
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

    // TODO: find a better solution for passing in additional principals
    private Subject getCombinedSubject() {
        Subject accSubject = Subject.getSubject(AccessController.getContext());
        if (accSubject == null) {
            return subject;
        }
        else {
            Subject combinedSubject = new Subject(false,
                    subject.getPrincipals(), subject.getPublicCredentials(), subject.getPrivateCredentials());
            combinedSubject.getPrincipals().addAll(accSubject.getPrincipals());
            combinedSubject.getPrivateCredentials().addAll(accSubject.getPrivateCredentials());
            combinedSubject.getPublicCredentials().addAll((accSubject.getPublicCredentials()));
            return combinedSubject;
        }
    }

    @Override
    public boolean hasPendingChanges() {
        checkLive();
        return !getBaseState().equals(rootTree.getNodeState());
    }

    @Nonnull
    public ChangeExtractor getChangeExtractor() {
        checkLive();
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

    @Override
    public SessionQueryEngine getQueryEngine() {
        checkLive();
        return new SessionQueryEngineImpl(indexProvider) {
            @Override
            protected NodeState getRootNodeState() {
                return rootTree.getNodeState();
            }

            @Override
            protected Root getRoot() {
                return RootImpl.this;
            }
        };
    }

    @Nonnull
    @Override
    public BlobFactory getBlobFactory() {
        return new BlobFactory() {
            @Override
            public Blob createBlob(InputStream inputStream) throws IOException {
                checkLive();
                return store.createBlob(inputStream);
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
        return branch.getRoot().builder();
    }

    // TODO better way to determine purge limit. See OAK-175
    void updated() {
        if (++modCount > PURGE_LIMIT) {
            modCount = 0;
            purgePendingChanges();
        }
    }

    CompiledPermissions getPermissions() {
        return accConfiguration.getCompiledPermissions(store, subject.getPrincipals());
    }

    //------------------------------------------------------------< private >---

    /**
     * Purge all pending changes to the underlying {@link NodeStoreBranch}.
     */
    private void purgePendingChanges() {
        branch.setRoot(rootTree.getNodeState());
        reset();
    }

    /**
     * Reset the root builder to the branch's current root state
     */
    private void reset() {
        rootTree.getNodeBuilder().reset(branch.getRoot());
    }

}
