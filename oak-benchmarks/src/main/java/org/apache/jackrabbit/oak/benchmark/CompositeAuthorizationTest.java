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
package org.apache.jackrabbit.oak.benchmark;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Repository;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Test the effect of multiple authorization configurations on the general read
 * operations.
 */
public class CompositeAuthorizationTest extends ReadDeepTreeTest {

    private int cnt;

    protected CompositeAuthorizationTest(boolean runAsAdmin, int cntConfigurations) {
        super(runAsAdmin, 1000, false);
        cnt =  cntConfigurations;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    return new Jcr(oak).with(newTestSecurityProvider(cnt));
                }
            });
        } else {
            return super.createRepository(fixture);
        }
    }

    private static SecurityProvider newTestSecurityProvider(int cnt) {
        SecurityProvider delegate = new SecurityProviderBuilder().build();

        AuthorizationConfiguration authorizationConfiguration = delegate
                .getConfiguration(AuthorizationConfiguration.class);
        if (!(authorizationConfiguration instanceof CompositeAuthorizationConfiguration)) {
            throw new IllegalStateException();
        } else {
            CompositeAuthorizationConfiguration composite = (CompositeAuthorizationConfiguration) authorizationConfiguration;
            final AuthorizationConfiguration defConfig = checkNotNull(composite.getDefaultConfig());
            for (int i = 0; i < cnt; i++) {
                composite.addConfiguration(new TmpAuthorizationConfig(defConfig));
            }
            composite.addConfiguration(defConfig);
        }
        return delegate;
    }

    private static final class TmpAuthorizationConfig implements AuthorizationConfiguration {

        private final AuthorizationConfiguration defConfig;

        private TmpAuthorizationConfig(@Nonnull AuthorizationConfiguration defConfig) {
            this.defConfig = defConfig;
        }

        @Nonnull
        @Override
        public AccessControlManager getAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
            return defConfig.getAccessControlManager(root, namePathMapper);
        }

        @Nonnull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return defConfig.getRestrictionProvider();
        }

        @Nonnull
        @Override
        public PermissionProvider getPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName, @Nonnull Set<Principal> principals) {
            return new TmpPermissionProvider(root);
        }

        @Nonnull
        @Override
        public String getName() {
            return defConfig.getName();
        }

        @Nonnull
        @Override
        public ConfigurationParameters getParameters() {
            return defConfig.getParameters();
        }

        @Nonnull
        @Override
        public WorkspaceInitializer getWorkspaceInitializer() {
            return WorkspaceInitializer.DEFAULT;
        }

        @Nonnull
        @Override
        public RepositoryInitializer getRepositoryInitializer() {
            return RepositoryInitializer.DEFAULT;
        }

        @Nonnull
        @Override
        public List<? extends CommitHook> getCommitHooks(@Nonnull String workspaceName) {
            return Collections.EMPTY_LIST;
        }

        @Nonnull
        @Override
        public List<? extends ValidatorProvider> getValidators(@Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
            return Collections.EMPTY_LIST;
        }

        @Nonnull
        @Override
        public List<ThreeWayConflictHandler> getConflictHandlers() {
            return Collections.EMPTY_LIST;
        }

        @Nonnull
        @Override
        public List<ProtectedItemImporter> getProtectedItemImporters() {
            return Collections.EMPTY_LIST;
        }

        @Nonnull
        @Override
        public Context getContext() {
            return defConfig.getContext();
        }
    }

    private static final class TmpPermissionProvider implements AggregatedPermissionProvider {

        private static final String POLICY_NAME = "customPolicy";

        private Root root;
        private Root immutableRoot;

        private TmpPermissionProvider(Root root) {
            this.root = root;
            immutableRoot = RootFactory.createReadOnlyRoot(root);
        }
        @Override
        public void refresh() {
            immutableRoot = RootFactory.createReadOnlyRoot(root);
        }

        @Nonnull
        @Override
        public Set<String> getPrivileges(@Nullable Tree tree) {
            performSomeRead(tree);
            return Collections.singleton(PrivilegeConstants.JCR_ALL);
        }

        @Override
        public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
            performSomeRead(tree);
            return true;
        }

        @Nonnull
        @Override
        public RepositoryPermission getRepositoryPermission() {
            return RepositoryPermission.ALL;
        }

        @Nonnull
        @Override
        public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
            performSomeRead(tree);
            return TreePermission.ALL;
        }

        @Override
        public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
            performSomeRead(tree);
            return true;
        }

        @Override
        public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
            performSomeRead(immutableRoot.getTree(oakPath));
            return true;
        }

        private void performSomeRead(@Nullable Tree tree) {
//            Tree immutableTree = PermissionUtil.getImmutableTree(tree, immutableRoot);
//            if (immutableTree != null) {
//                immutableTree.hasChild(POLICY_NAME);
//            }
        }

        @Nonnull
        @Override
        public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
            return (privilegeBits != null) ? privilegeBits : new PrivilegeBitsProvider(immutableRoot).getBits(PrivilegeConstants.JCR_ALL);
        }

        @Override
        public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
            return permissions;
        }

        @Override
        public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
            return permissions;
        }

        @Override
        public long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
            return permissions;
        }

        @Override
        public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
            return true;
        }

        @Nonnull
        @Override
        public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parentPermission) {
            return getTreePermission(tree, parentPermission);
        }
    }
}