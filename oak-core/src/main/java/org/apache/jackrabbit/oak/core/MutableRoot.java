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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.LazyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.index.diffindex.UUIDDiffIndexProviderWrapper;
import org.apache.jackrabbit.oak.query.ExecutionContext;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionAware;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.PrefetchNodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class MutableRoot implements Root, PermissionAware {

    /**
     * The underlying store to which this root belongs
     */
    private final NodeStore store;

    private final CommitHook hook;

    private final String workspaceName;

    private final Subject subject;

    private final SecurityProvider securityProvider;
    
    private final QueryEngineSettings queryEngineSettings;

    private final QueryIndexProvider indexProvider;

    private final ContentSessionImpl session;

    /**
     * Current root {@code Tree}
     */
    private final MutableTree rootTree;

    /**
     * Unsecured builder for the root tree
     */
    private final NodeBuilder builder;

    /**
     * Secured builder for the root tree
     */
    private final SecureNodeBuilder secureBuilder;

    /**
     * Sentinel for the next move operation to take place on this root
     */
    private Move lastMove;

    /**
     * Simple info object used to collect all move operations (source + dest)
     * for further processing in those commit hooks that wish to distinguish
     * between simple add/remove and move operations.
     * Please note that this information will only allow to perform best-effort
     * matching as depending on the sequence of modifications some operations
     * may no longer be detected as changes in the commit hook due to way the
     * diff is compiled.
     */
    private final MoveTracker moveTracker = new MoveTracker();

    /**
     * Number of {@link #updated} occurred.
     */
    private long modCount;

    private final LazyValue<PermissionProvider> permissionProvider = new LazyValue<PermissionProvider>() {
        @Override
        protected PermissionProvider createValue() {
            return getAcConfig().getPermissionProvider(
                    MutableRoot.this,
                    getContentSession().getWorkspaceName(),
                    subject.getPrincipals());
        }
    };

    private static final boolean CLASSIC_MOVE =
            SystemPropertySupplier.create("oak.classicMove", false).get();

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
    MutableRoot(NodeStore store,
                 CommitHook hook,
                 String workspaceName,
                 Subject subject,
                 SecurityProvider securityProvider,
                 QueryEngineSettings queryEngineSettings,
                 QueryIndexProvider indexProvider,
                 Feature classicMove,
                 ContentSessionImpl session) {
        this.store = requireNonNull(store);
        this.hook = requireNonNull(hook);
        this.workspaceName = requireNonNull(workspaceName);
        this.subject = requireNonNull(subject);
        this.securityProvider = requireNonNull(securityProvider);
        this.queryEngineSettings = queryEngineSettings;
        this.indexProvider = indexProvider;
        this.session = requireNonNull(session);
        this.lastMove = createMove(classicMove);

        builder = store.getRoot().builder();
        secureBuilder = new SecureNodeBuilder(builder, permissionProvider);
        rootTree = new MutableTree(this, secureBuilder, lastMove);
    }

    /**
     * Called whenever a method on this instance or on any {@code Tree} instance
     * obtained from this {@code Root} is called. Throws an exception if this
     * {@code Root} instance is not live anymore (e.g. because the session has
     * been logged out already).
     */
    void checkLive() {
        session.checkLive();
    }

    //---------------------------------------------------------------< Root >---

    @NotNull
    @Override
    public ContentSession getContentSession() {
        return session;
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        if (isAncestor(requireNonNull(sourcePath), requireNonNull(destPath))) {
            return false;
        } else if (sourcePath.equals(destPath)) {
            return true;
        }

        checkLive();
        MutableTree source = rootTree.getTree(sourcePath);
        if (!source.exists()) {
            return false;
        }

        String newName = getName(destPath);
        MutableTree newParent = rootTree.getTree(getParentPath(destPath));
        if (!newParent.exists() || newParent.hasChild(newName)) {
            return false;
        }

        boolean success = source.moveTo(newParent, newName);
        if (success) {
            lastMove = lastMove.setMove(sourcePath, newParent, newName);
            updated();
            // remember all move operations for further processing in the commit hooks.
            moveTracker.addMove(sourcePath, destPath);
        }
        return success;
    }

    @NotNull
    @Override
    public MutableTree getTree(@NotNull String path) {
        checkLive();
        return rootTree.getTree(path);
    }

    @Override
    public void rebase() {
        checkLive();
        store.rebase(builder);
        secureBuilder.baseChanged();
        if (permissionProvider.hasValue()) {
            permissionProvider.get().refresh();
        }
    }

    @Override
    public final void refresh() {
        checkLive();
        store.reset(builder);
        secureBuilder.baseChanged();
        modCount = 0;
        if (permissionProvider.hasValue()) {
            permissionProvider.get().refresh();
        }
    }

    @Override
    public void commit(@NotNull Map<String, Object> info) throws CommitFailedException {
        checkLive();
        ContentSession session = getContentSession();
        CommitInfo commitInfo = new CommitInfo(
                session.toString(), session.getAuthInfo().getUserID(), newInfoWithCommitContext(info));
        store.merge(builder, getCommitHook(), commitInfo);
        secureBuilder.baseChanged();
        modCount = 0;
        if (permissionProvider.hasValue()) {
            permissionProvider.get().refresh();
        }
        moveTracker.clear();
    }

    @Override
    public void commit() throws CommitFailedException {
        commit(Collections.<String, Object>emptyMap());
    }

    /**
     * Combine the globally defined commit hook(s) and the hooks and validators defined by the
     * various security related configurations.
     *
     * @return A commit hook combining repository global commit hook(s) with the pluggable hooks
     *         defined with the security modules and the padded {@code hooks}.
     */
    private CommitHook getCommitHook() {
        List<CommitHook> hooks = new ArrayList<>();
        hooks.add(ResetCommitAttributeHook.INSTANCE);
        hooks.add(hook);

        List<CommitHook> postValidationHooks = new ArrayList<CommitHook>();
        List<ValidatorProvider> validators = new ArrayList<>();

        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            for (CommitHook ch : sc.getCommitHooks(workspaceName)) {
                if (ch instanceof PostValidationHook) {
                    postValidationHooks.add(ch);
                } else if (ch != EmptyHook.INSTANCE) {
                    hooks.add(ch);
                }
            }

            validators.addAll(sc.getValidators(workspaceName, subject.getPrincipals(), moveTracker));
        }

        if (!validators.isEmpty()) {
            hooks.add(new EditorHook(CompositeEditorProvider.compose(validators)));
        }
        hooks.addAll(postValidationHooks);

        return CompositeHook.compose(hooks);
    }

    @Override
    public boolean hasPendingChanges() {
        checkLive();
        return modCount > 0;
    }

    @NotNull
    @Override
    public QueryEngine getQueryEngine() {
        checkLive();
        return new QueryEngineImpl() {
            @Override
            protected ExecutionContext getExecutionContext() {
                QueryIndexProvider provider = indexProvider;
                if (hasPendingChanges()) {
                    provider = new UUIDDiffIndexProviderWrapper(
                            provider, getBaseState(), getRootState());
                }
                return new ExecutionContext(
                        getBaseState(),
                        MutableRoot.this,
                        queryEngineSettings,
                        provider,
                        permissionProvider.get(),
                        store instanceof PrefetchNodeStore ?
                                (PrefetchNodeStore) store :
                                PrefetchNodeStore.NOOP
                );
            }
        };
    }

    @Override @NotNull
    public Blob createBlob(@NotNull InputStream inputStream) throws IOException {
        checkLive();
        return store.createBlob(requireNonNull(inputStream));
    }

    @Override
    public Blob getBlob(@NotNull String reference) {
        return store.getBlob(reference);
    }

    //-----------------------------------------------------------< internal >---

    /**
     * Returns the node state from the time this root was created, that
     * is this root's base state.
     *
     * @return base node state
     */
    @NotNull
    NodeState getBaseState() {
        return builder.getBaseState();
    }

    void updated() {
        modCount++;
    }

    //------------------------------------------------------------< private >---

    /**
     * Root node state of the tree including all transient changes at the time of
     * this call.
     *
     * @return root node state
     */
    @NotNull
    private NodeState getRootState() {
        return builder.getNodeState();
    }

    @NotNull
    private AuthorizationConfiguration getAcConfig() {
        return securityProvider.getConfiguration(AuthorizationConfiguration.class);
    }

    private static Map<String, Object> newInfoWithCommitContext(Map<String, Object> info){
        return ImmutableMap.<String, Object>builder()
                .putAll(info)
                .put(CommitContext.NAME, new SimpleCommitContext())
                .build();
    }

    //--------------------------------------------------------------------------------------------< PermissionAware >---
    @NotNull
    @Override
    public PermissionProvider getPermissionProvider() {
        return permissionProvider.get();
    }

    //---------------------------------------------------------< MoveRecord >---

    /**
     * Instances implementing this interface record move operations which took place on this root.
     * They form a singly linked list where each move instance points to the next one.
     * The last entry in the list is always an empty slot to be filled in by calling
     * {@code setMove()}. This fills the slot with the source and destination of the move
     * and links this move to the next one which will be the new empty slot.
     * <p>
     * Moves can be applied to {@code MutableTree} instances by calling {@code apply()},
     * which will execute all moves in the list on the passed tree instance
     */
    interface Move {

        /**
         * Set this move to the given source and destination. Creates a new empty
         * slot, sets this as the next move and returns it.
         */
        Move setMove(String source, MutableTree destParent, String destName);

        /**
         * Apply this and all subsequent moves to the passed tree instance.
         */
        Move apply(MutableTree tree);
    }

    Move createMove(@Nullable Feature classicMove) {
        // default implementation is memory reduced move, unless classic
        // implementation is enabled by system property or feature toggle
        if (CLASSIC_MOVE || (classicMove != null && classicMove.isEnabled())) {
            return new ClassicMove();
        } else {
            return new NeoMove();
        }
    }

    static class ClassicMove implements Move {

        /**
         * source path
         */
        private String source;

        /**
         * Parent tree of the destination
         */
        private MutableTree destParent;

        /**
         * Name at the destination
         */
        private String destName;

        /**
         * Pointer to the next move. {@code null} if this is the last, empty slot
         */
        private ClassicMove next;

        @Override
        public Move setMove(String source,
                            MutableTree destParent,
                            String destName) {
            this.source = source;
            this.destParent = destParent;
            this.destName = destName;
            return next = new ClassicMove();
        }

        @Override
        public Move apply(MutableTree tree) {
            ClassicMove move = this;
            while (move.next != null) {
                if (move.source.equals(tree.getPathInternal())) {
                    tree.setParentAndName(move.destParent, move.destName);
                }
                move = move.next;
            }
            return move;
        }

        @Override
        public String toString() {
            return source == null
                    ? "NIL"
                    : '>' + source + ':' + PathUtils.concat(destParent.getPathInternal(), destName);
        }
    }

    class NeoMove implements Move {

        /**
         * Source path
         */
        private String source;

        /**
         * Destination path
         */
        private String destination;

        /**
         * Pointer to the next move. {@code null} if this is the last, empty slot
         */
        private NeoMove next;


        @Override
        public Move setMove(String source, MutableTree destParent, String destName) {
            this.destination = concat(destParent.getPathInternal(), destName);
            this.source = source;
            this.next = new NeoMove();
            return next;
        }

        @Override
        public Move apply(MutableTree tree) {
            NeoMove move = this;
            String path = null;
            while (move.next != null) {
                if (path == null) {
                    // initialize once with path from tree
                    path = tree.getPathInternal();
                }
                path = move.rewrite(path);
                move = move.next;
            }
            if (path != null && !path.equals(tree.getPathInternal())) {
                // at least one move was applied
                tree.setParentAndName(getTree(getParentPath(path)), getName(path));
            }
            return move;
        }

        private String rewrite(String path) {
            if (path.equals(source)) {
                return destination;
            } else if (isAncestor(source, path)) {
                return destination + path.substring(source.length());
            } else {
                return path;
            }
        }

        @Override
        public String toString() {
            return source == null
                    ? "NIL"
                    : '>' + source + ':' + destination;
        }
    }

}
