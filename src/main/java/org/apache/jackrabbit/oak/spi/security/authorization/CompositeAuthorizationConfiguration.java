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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * {@link CompositeAuthorizationConfiguration} that combines different
 * authorization models.
 */
public class CompositeAuthorizationConfiguration extends CompositeConfiguration<AuthorizationConfiguration> implements AuthorizationConfiguration {

    public CompositeAuthorizationConfiguration(@Nonnull SecurityProvider securityProvider) {
        super(AuthorizationConfiguration.NAME, securityProvider);
    }

    @Nonnull
    @Override
    public AccessControlManager getAccessControlManager(final @Nonnull Root root,
                                                        final @Nonnull NamePathMapper namePathMapper) {
        List<AccessControlManager> mgrs = Lists.transform(getConfigurations(), new Function<AuthorizationConfiguration, AccessControlManager>() {
            @Override
            public AccessControlManager apply(AuthorizationConfiguration authorizationConfiguration) {
                return authorizationConfiguration.getAccessControlManager(root, namePathMapper);
            }
        });
        return new CompositeAcMgr(root, namePathMapper, getSecurityProvider(), mgrs);
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return CompositeRestrictionProvider.newInstance(
                Lists.transform(getConfigurations(),
                        new Function<AuthorizationConfiguration, RestrictionProvider>() {
                            @Override
                            public RestrictionProvider apply(AuthorizationConfiguration authorizationConfiguration) {
                                return authorizationConfiguration.getRestrictionProvider();
                            }
                        }));
    }

    @Nonnull
    @Override
    public PermissionProvider getPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName, @Nonnull Set<Principal> principals) {
        // TODO
        throw new UnsupportedOperationException("not yet implemented.");
    }

    /**
     *
     */
    private static class CompositeAcMgr extends AbstractAccessControlManager {

        private final List<AccessControlManager> acMgrs;

        private CompositeAcMgr(@Nonnull Root root,
                               @Nonnull NamePathMapper namePathMapper,
                               @Nonnull SecurityProvider securityProvider,
                               @Nonnull List<AccessControlManager> acMgrs) {
            super(root, namePathMapper, securityProvider);
            this.acMgrs = acMgrs;
        }

        //-------------------------------------------< AccessControlManager >---
        @Override
        public Privilege[] getSupportedPrivileges(String absPath) throws RepositoryException {
            ImmutableList.Builder<Privilege> privs = ImmutableList.builder();
            for (AccessControlManager acMgr : acMgrs) {
                privs.add(acMgr.getSupportedPrivileges(absPath));
            }
            List<Privilege> l = privs.build();
            return l.toArray(new Privilege[l.size()]);
        }

        @Override
        public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
            ImmutableList.Builder<AccessControlPolicy> privs = ImmutableList.builder();
            for (AccessControlManager acMgr : acMgrs) {
                privs.add(acMgr.getPolicies(absPath));
            }
            List<AccessControlPolicy> l = privs.build();
            return l.toArray(new AccessControlPolicy[l.size()]);
        }

        @Override
        public AccessControlPolicy[] getEffectivePolicies(String absPath) throws RepositoryException {
            ImmutableList.Builder<AccessControlPolicy> privs = ImmutableList.builder();
            for (AccessControlManager acMgr : acMgrs) {
                privs.add(acMgr.getEffectivePolicies(absPath));
            }
            List<AccessControlPolicy> l = privs.build();
            return l.toArray(new AccessControlPolicy[l.size()]);
        }

        @Override
        public AccessControlPolicyIterator getApplicablePolicies(String absPath) throws RepositoryException {
            List<AccessControlPolicyIterator> l = Lists.newArrayList();
            for (AccessControlManager acMgr : acMgrs) {
                l.add(acMgr.getApplicablePolicies(absPath));
            }
            return new AccessControlPolicyIteratorAdapter(Iterators.concat(l.toArray(new AccessControlPolicyIterator[l.size()])));
        }

        @Override
        public void setPolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
            // TODO
            throw new UnsupportedOperationException("not yet implemented.");
        }

        @Override
        public void removePolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
            // TODO
            throw new UnsupportedOperationException("not yet implemented.");
        }

        //---------------------------------< JackrabbitAccessControlManager >---
        @Override
        public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) throws RepositoryException {
            ImmutableList.Builder<JackrabbitAccessControlPolicy> policies = ImmutableList.builder();
            for (AccessControlManager acMgr : acMgrs) {
                if (acMgr instanceof JackrabbitAccessControlManager) {
                    policies.add(((JackrabbitAccessControlManager) acMgr).getApplicablePolicies(principal));
                }
            }
            List<JackrabbitAccessControlPolicy> l = policies.build();
            return l.toArray(new JackrabbitAccessControlPolicy[l.size()]);
        }

        @Override
        public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) throws RepositoryException {
            ImmutableList.Builder<JackrabbitAccessControlPolicy> privs = ImmutableList.builder();
            for (AccessControlManager acMgr : acMgrs) {
                if (acMgr instanceof JackrabbitAccessControlManager) {
                    privs.add(((JackrabbitAccessControlManager) acMgr).getPolicies(principal));
                }
            }
            List<JackrabbitAccessControlPolicy> l = privs.build();
            return l.toArray(new JackrabbitAccessControlPolicy[l.size()]);
        }

        @Override
        public AccessControlPolicy[] getEffectivePolicies(Set<Principal> principals) throws RepositoryException {
            ImmutableList.Builder<AccessControlPolicy> privs = ImmutableList.builder();
            for (AccessControlManager acMgr : acMgrs) {
                if (acMgr instanceof JackrabbitAccessControlManager) {
                    privs.add(((JackrabbitAccessControlManager) acMgr).getEffectivePolicies(principals));
                }
            }
            List<AccessControlPolicy> l = privs.build();
            return l.toArray(new AccessControlPolicy[l.size()]);
        }
    }
}