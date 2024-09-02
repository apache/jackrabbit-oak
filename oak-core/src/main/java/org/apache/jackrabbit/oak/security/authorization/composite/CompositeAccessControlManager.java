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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Access control manager that aggregates a list of different access control
 * manager implementations. Note, that the implementations *must* implement
 * the {@link org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner}
 * interface in order to be able to set and remove individual access control
 * policies.
 */
class CompositeAccessControlManager extends AbstractAccessControlManager {

    private final List<AccessControlManager> acMgrs;
    private final AggregationFilter aggregationFilter;

    public CompositeAccessControlManager(@NotNull Root root,
                                         @NotNull NamePathMapper namePathMapper,
                                         @NotNull SecurityProvider securityProvider,
                                         @NotNull List<AccessControlManager> acMgrs,
                                         @NotNull AggregationFilter aggregationFilter) {
        super(root, namePathMapper, securityProvider);
        this.acMgrs = acMgrs;
        this.aggregationFilter = aggregationFilter;
    }

    //-----------------------------------------------< AccessControlManager >---
    @NotNull
    @Override
    public Privilege[] getSupportedPrivileges(String absPath) throws RepositoryException {
        ImmutableSet.Builder<Privilege> privs = ImmutableSet.builder();
        for (AccessControlManager acMgr : acMgrs) {
            privs.add(acMgr.getSupportedPrivileges(absPath));
        }
        Set<Privilege> s = privs.build();
        return s.toArray(new Privilege[0]);
    }

    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
        ImmutableList.Builder<AccessControlPolicy> policies = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            policies.add(acMgr.getPolicies(absPath));
        }
        List<AccessControlPolicy> l = policies.build();
        return l.toArray(new AccessControlPolicy[0]);
    }

    @Override
    public AccessControlPolicy[] getEffectivePolicies(String absPath) throws RepositoryException {
        ImmutableList.Builder<AccessControlPolicy> policies = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            policies.add(acMgr.getEffectivePolicies(absPath));
            if (aggregationFilter.stop(acMgr, absPath)) {
                break;
            }
        }
        return policies.build().stream().distinct().toArray(AccessControlPolicy[]::new);
    }

    @Override
    public AccessControlPolicyIterator getApplicablePolicies(String absPath) throws RepositoryException {
        List<AccessControlPolicyIterator> l = new ArrayList<>();
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof PolicyOwner) {
                l.add(acMgr.getApplicablePolicies(absPath));
            }
        }
        return new AccessControlPolicyIteratorAdapter(Iterators.concat(l.toArray(new AccessControlPolicyIterator[0])));
    }

    @Override
    public void setPolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof PolicyOwner && ((PolicyOwner) acMgr).defines(absPath, policy)) {
                acMgr.setPolicy(absPath, policy);
                return;
            }
        }
        throw new AccessControlException("Cannot set access control policy " + policy + "; no PolicyOwner found.");
    }

    @Override
    public void removePolicy(String absPath, AccessControlPolicy policy) throws RepositoryException {
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof PolicyOwner && ((PolicyOwner) acMgr).defines(absPath, policy)) {
                acMgr.removePolicy(absPath, policy);
                return;
            }
        }
        throw new AccessControlException("Cannot remove access control policy " + policy + "; no PolicyOwner found.");
    }

    //-------------------------------------< JackrabbitAccessControlManager >---
    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(@NotNull Principal principal) throws RepositoryException {
        ImmutableList.Builder<JackrabbitAccessControlPolicy> policies = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof JackrabbitAccessControlManager && acMgr instanceof PolicyOwner) {
                policies.add(((JackrabbitAccessControlManager) acMgr).getApplicablePolicies(principal));
            }
        }
        List<JackrabbitAccessControlPolicy> l = policies.build();
        return l.toArray(new JackrabbitAccessControlPolicy[0]);
    }

    @NotNull
    @Override
    public JackrabbitAccessControlPolicy[] getPolicies(@NotNull Principal principal) throws RepositoryException {
        ImmutableList.Builder<JackrabbitAccessControlPolicy> policies = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof JackrabbitAccessControlManager) {
                policies.add(((JackrabbitAccessControlManager) acMgr).getPolicies(principal));
            }
        }
        List<JackrabbitAccessControlPolicy> l = policies.build();
        return l.toArray(new JackrabbitAccessControlPolicy[0]);
    }

    @NotNull
    @Override
    public AccessControlPolicy[] getEffectivePolicies(@NotNull Set<Principal> principals) throws RepositoryException {
        ImmutableList.Builder<AccessControlPolicy> policies = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof JackrabbitAccessControlManager) {
                JackrabbitAccessControlManager jAcMgr = (JackrabbitAccessControlManager) acMgr;
                policies.add(jAcMgr.getEffectivePolicies(principals));
                if (aggregationFilter.stop(jAcMgr, principals)) {
                    break;
                }
            }
        }
        List<AccessControlPolicy> l = policies.build();
        return l.toArray(new AccessControlPolicy[0]);
    }

    @Override
    public @NotNull Iterator<AccessControlPolicy> getEffectivePolicies(@NotNull Set<Principal> principals, @Nullable String... absPaths) throws AccessDeniedException, AccessControlException, UnsupportedRepositoryOperationException, RepositoryException {
        ImmutableList.Builder<Iterator<AccessControlPolicy>> iterators = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof JackrabbitAccessControlManager) {
                JackrabbitAccessControlManager jAcMgr = (JackrabbitAccessControlManager) acMgr;
                iterators.add(jAcMgr.getEffectivePolicies(principals, absPaths));
                if (aggregationFilter.stop(jAcMgr, principals)) {
                    break;
                }
            }
        }
        return Iterators.concat(iterators.build().toArray(new Iterator[0]));
    }
}
