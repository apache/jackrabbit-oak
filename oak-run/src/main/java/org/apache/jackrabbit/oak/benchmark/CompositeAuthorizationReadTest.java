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
import java.util.Iterator;
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
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.OpenPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * CompositeAuthorizationReadTest... TODO
 */
public class CompositeAuthorizationReadTest extends ReadDeepTreeTest {

    private int cnt;

    protected CompositeAuthorizationReadTest(boolean runAsAdmin, int cntConfigurations) {
        super(runAsAdmin, 1000, false);
        cnt =  cntConfigurations;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    return new Jcr(oak).with(new TmpSecurityProvider(cnt));
                }
            });
        } else {
            return super.createRepository(fixture);
        }
    }

    private static final class TmpSecurityProvider extends SecurityProviderImpl {

        private final int cnt;

        private TmpSecurityProvider(int cnt) {
            this.cnt = cnt;
        }

        @Nonnull
        @Override
        public ConfigurationParameters getParameters(@Nullable String name) {
            return ConfigurationParameters.EMPTY;
        }

        @Nonnull
        @Override
        public Iterable<? extends SecurityConfiguration> getConfigurations() {
            Set<SecurityConfiguration> configs = (Set<SecurityConfiguration>) super.getConfigurations();

            CompositeAuthorizationConfiguration composite = new CompositeAuthorizationConfiguration(this);
            Iterator<SecurityConfiguration> it = configs.iterator();
            AuthorizationConfiguration base = null;
            while (it.hasNext()) {
                SecurityConfiguration sc = it.next();
                if (sc instanceof AuthorizationConfiguration) {
                    base = (AuthorizationConfiguration) sc;
                    it.remove();
                    break;
                }
            }
            fillComposite(composite, base, cnt);
            configs.add(composite);

            return configs;
        }

        @Nonnull
        @Override
        public <T> T getConfiguration(@Nonnull Class<T> configClass) {
            T c = super.getConfiguration(configClass);
            if (AuthorizationConfiguration.class == configClass) {
                CompositeAuthorizationConfiguration composite = new CompositeAuthorizationConfiguration(this);
                fillComposite(composite, (AuthorizationConfiguration) c, cnt);
                return (T) composite;
            } else {
                return c;
            }
        }

        private static void fillComposite(CompositeAuthorizationConfiguration composite,
                                          final AuthorizationConfiguration base,
                                          int cnt) {
            composite.addConfiguration(base);
            for (int i = 0; i < cnt; i++) {
                composite.addConfiguration(new AuthorizationConfiguration() {

                    @Nonnull
                    @Override
                    public AccessControlManager getAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
                        return base.getAccessControlManager(root, namePathMapper);
                    }

                    @Nonnull
                    @Override
                    public RestrictionProvider getRestrictionProvider() {
                        return base.getRestrictionProvider();
                    }

                    @Nonnull
                    @Override
                    public PermissionProvider getPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName, @Nonnull Set<Principal> principals) {
                        return new TmpPermissionProvider(root);
                    }

                    @Nonnull
                    @Override
                    public String getName() {
                        return base.getName();
                    }

                    @Nonnull
                    @Override
                    public ConfigurationParameters getParameters() {
                        return base.getParameters();
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
                    public List<ProtectedItemImporter> getProtectedItemImporters() {
                        return Collections.EMPTY_LIST;
                    }

                    @Nonnull
                    @Override
                    public Context getContext() {
                        return base.getContext();
                    }
                });
            }

        }
    }

    private static final class TmpPermissionProvider implements PermissionProvider {

        private static final String POLICY_NAME = "customPolicy";

        private Root root;
        private Root immutableRoot;

        private final PermissionProvider fake = OpenPermissionProvider.getInstance();

        private TmpPermissionProvider(Root root) {
            this.root = root;
            immutableRoot = RootFactory.createReadOnlyRoot(root);
        }
        @Override
        public void refresh() {
            root.refresh();
            immutableRoot = RootFactory.createReadOnlyRoot(root);
        }

        @Nonnull
        @Override
        public Set<String> getPrivileges(@Nullable Tree tree) {
            performSomeRead(tree);
            return fake.getPrivileges(tree);
        }

        @Override
        public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
            performSomeRead(tree);
            return fake.hasPrivileges(tree, privilegeNames);
        }

        @Nonnull
        @Override
        public RepositoryPermission getRepositoryPermission() {
            return fake.getRepositoryPermission();
        }

        @Nonnull
        @Override
        public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
            performSomeRead(tree);
            return fake.getTreePermission(tree, parentPermission);
        }

        @Override
        public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
            performSomeRead(tree);
            return fake.isGranted(tree, property, permissions);
        }

        @Override
        public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
            performSomeRead(immutableRoot.getTree(oakPath));
            return fake.isGranted(oakPath, jcrActions);
        }

        private void performSomeRead(@Nullable Tree tree) {
            Tree immutableTree = PermissionUtil.getImmutableTree(tree, immutableRoot);
            if (immutableTree != null) {
                immutableTree.hasChild(POLICY_NAME);
            }
        }
    }
}