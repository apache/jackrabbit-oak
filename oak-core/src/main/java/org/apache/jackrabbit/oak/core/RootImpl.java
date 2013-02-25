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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.BlobFactory;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.diffindex.UUIDDiffIndexProviderWrapper;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.observation.ChangeExtractor;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
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

    /**
     * The underlying store to which this root belongs
     */
    private final NodeStore store;

    private final String workspaceName;

    private final CommitHook hook;

    private final Subject subject;

    /**
     * The security provider.
     */
    private final SecurityProvider securityProvider;

    /**
     * Current branch this root operates on
     */
    private NodeStoreBranch branch;

    /**
     * Current root {@code Tree}
     */
    private TreeImpl rootTree;

    /**
     * Number of {@link #updated} occurred so since the lase
     * purge.
     */
    private int modCount;

    private final QueryIndexProvider indexProvider;

    /**
     * New instance bases on a given {@link NodeStore} and a workspace
     *
     * @param store            node store
     * @param hook             the commit hook
     * @param workspaceName    name of the workspace
     * @param subject          the subject.
     * @param securityProvider the security configuration.
     * @param indexProvider    the query index provider.
     */
    public RootImpl(NodeStore store,
                    CommitHook hook,
                    String workspaceName,
                    Subject subject,
                    SecurityProvider securityProvider,
                    QueryIndexProvider indexProvider) {
        this.store = checkNotNull(store);
        this.workspaceName = checkNotNull(workspaceName);
        this.hook = checkNotNull(hook);
        this.subject = checkNotNull(subject);
        this.securityProvider = checkNotNull(securityProvider);
        this.indexProvider = indexProvider;
        refresh();
    }

    // TODO: review if this constructor really makes sense and cannot be replaced.
    public RootImpl(NodeStore store) {
        this.store = checkNotNull(store);
        // FIXME: define proper default or pass workspace name with the constructor
        this.workspaceName = Oak.DEFAULT_WORKSPACE_NAME;
        this.hook = EmptyHook.INSTANCE;
        this.subject = SystemSubject.INSTANCE;
        this.securityProvider = new OpenSecurityProvider();
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
        RootImpl root = new RootImpl(store, hook, workspaceName, subject, securityProvider, getIndexProvider()) {
            @Override
            protected void checkLive() {
                RootImpl.this.checkLive();
            }
        };
        return root;
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
        checkArgument(PathUtils.isAbsolute(path), "Not an absolute path: " + path);
        return rootTree.getLocation().getChild(path.substring(1));
    }

    @Override
    public void rebase() {
        checkLive();
        if (!store.getRoot().equals(rootTree.getBaseState())) {
            purgePendingChanges();
            branch.rebase();
            rootTree = TreeImpl.createRoot(this);
        }
    }

    @Override
    public final void refresh() {
        checkLive();
        branch = store.branch();
        rootTree = TreeImpl.createRoot(this);
        modCount = 0;
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
                    branch.merge(getCommitHook());
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

    /**
     * Combine the globally defined commit hook(s) with the hooks and
     * validators defined by the various security related configurations.
     *
     * @return A commit hook combining repository global commit hook(s) with
     *         the pluggable hooks defined with the security modules.
     */
    private CommitHook getCommitHook() {
        List<CommitHook> commitHooks = new ArrayList<CommitHook>();
        commitHooks.add(hook);
        List<CommitHook> securityHooks = new ArrayList<CommitHook>();
        for (SecurityConfiguration sc : securityProvider.getSecurityConfigurations()) {
            CommitHook validators = sc.getValidators().getCommitHook(workspaceName);
            if (validators != EmptyHook.INSTANCE) {
                commitHooks.add(validators);
            }
            CommitHook ch = sc.getSecurityHooks().getCommitHook(workspaceName);
            if (ch != EmptyHook.INSTANCE) {
                securityHooks.add(ch);
            }
        }
        commitHooks.addAll(securityHooks);
        return CompositeHook.compose(commitHooks);
    }

    // TODO: find a better solution for passing in additional principals
    private Subject getCombinedSubject() {
        Subject accSubject = Subject.getSubject(AccessController.getContext());
        if (accSubject == null) {
            return subject;
        } else {
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
    public QueryEngine getQueryEngine() {
        checkLive();
        return new QueryEngineImpl(getIndexProvider()) {

            @Override
            protected NodeState getRootState() {
                return rootTree.getNodeState();
            }

            @Override
            protected Tree getRootTree() {
                return rootTree;
            }

        };
    }

    @Nonnull
    @Override
    public BlobFactory getBlobFactory() {
        checkLive();

        return new BlobFactory() {
            @Override
            public Blob createBlob(InputStream inputStream) throws IOException {
                checkLive();
                return store.createBlob(inputStream);
            }
        };
    }

    private QueryIndexProvider getIndexProvider() {
        if (hasPendingChanges()) {
            return new UUIDDiffIndexProviderWrapper(indexProvider,
                    getBaseState(), rootTree.getNodeState());
        }
        return indexProvider;
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Returns the node state from which the current branch was created.
     *
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

    PermissionProvider getPermissionProvider() {
        return securityProvider.getAccessControlConfiguration().getPermissionProvider(this, subject.getPrincipals());
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
