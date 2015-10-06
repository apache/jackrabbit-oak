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
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;

/**
 * Access control manager that aggregates a list of different access control
 * manager implementations. Note, that the implementations *must* implement
 * the {@link org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner}
 * interface in order to be able to set and remove individual access control
 * policies.
 */
class CompositeAccessControlManager extends AbstractAccessControlManager {

    private final List<AccessControlManager> acMgrs;

    public CompositeAccessControlManager(@Nonnull Root root,
                                         @Nonnull NamePathMapper namePathMapper,
                                         @Nonnull SecurityProvider securityProvider,
                                         @Nonnull List<AccessControlManager> acMgrs) {
        super(root, namePathMapper, securityProvider);
        this.acMgrs = acMgrs;
    }

    //-----------------------------------------------< AccessControlManager >---
    @Nonnull
    @Override
    public Privilege[] getSupportedPrivileges(String absPath) throws RepositoryException {
        ImmutableSet.Builder<Privilege> privs = ImmutableSet.builder();
        for (AccessControlManager acMgr : acMgrs) {
            privs.add(acMgr.getSupportedPrivileges(absPath));
        }
        Set<Privilege> s = privs.build();
        return s.toArray(new Privilege[s.size()]);
    }

    @Override
    public AccessControlPolicy[] getPolicies(String absPath) throws RepositoryException {
        ImmutableList.Builder<AccessControlPolicy> policies = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            policies.add(acMgr.getPolicies(absPath));
        }
        List<AccessControlPolicy> l = policies.build();
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
            if (acMgr instanceof PolicyOwner) {
                l.add(acMgr.getApplicablePolicies(absPath));
            }
        }
        return new AccessControlPolicyIteratorAdapter(Iterators.concat(l.toArray(new AccessControlPolicyIterator[l.size()])));
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
    @Override
    public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) throws RepositoryException {
        ImmutableList.Builder<JackrabbitAccessControlPolicy> policies = ImmutableList.builder();
        for (AccessControlManager acMgr : acMgrs) {
            if (acMgr instanceof JackrabbitAccessControlManager && acMgr instanceof PolicyOwner) {
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