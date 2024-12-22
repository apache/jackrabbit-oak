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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalGroupPrincipalProvider.CACHE_PRINCIPAL_NAMES;
import static org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants.REP_CACHE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.jcr.RepositoryException;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.ServiceReference;

public class ExternalGroupPrincipalProviderWithCacheTest extends AbstractPrincipalTest {

    private @NotNull Group testGroup;

    private final String idpName;

    public ExternalGroupPrincipalProviderWithCacheTest() {
        this.idpName = "test";
    }

    @Before
    public void before() throws Exception {
        super.before();

        testGroup = createTestGroup();

        principalProvider = createPrincipalProvider(getSystemRoot(), getUserConfiguration());
        Iterator<ExternalGroup> externalGroup = idp.listGroups();
        while (externalGroup.hasNext()) {
            ExternalGroup next = externalGroup.next();
            Authorizable authorizable = getUserManager(root).getAuthorizable(next.getPrincipalName());
            testGroup.addMember(authorizable);
        }
        root.commit();
        root.refresh();
        getSystemRoot().refresh();
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.group().setDynamicGroups(true);
        config.user().setDynamicMembership(true);
        return config;
    }

    @Override
    @NotNull ExternalGroupPrincipalProvider createPrincipalProvider(@NotNull Root r, @NotNull UserConfiguration uc) {

        SyncHandlerMappingTracker mappingTracker = new SyncHandlerMappingTracker(context.bundleContext());
        SyncConfigTracker scTracker = spy(new SyncConfigTracker(context.bundleContext(), mappingTracker));

        if (!idpName.isEmpty()) {
            when(scTracker.getIdpNamesWithDynamicGroups()).thenReturn(Collections.singleton(idpName));
        } else {
            when(scTracker.getIdpNamesWithDynamicGroups()).thenReturn(Collections.emptySet());
        }
        when(scTracker.getServiceReferences()).thenReturn(new ServiceReference[]{mock(ServiceReference.class)});
        return new ExternalGroupPrincipalProvider(r, uc, getNamePathMapper(), scTracker);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME, ConfigurationParameters.of(Map.of(
                                CacheConstants.PARAM_CACHE_EXPIRATION, 10000,
                                CacheConstants.PARAM_CACHE_MAX_STALE, 10000,
                                ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT
                        )
                ));
    }

    @NotNull
    Set<Principal> getExternalGroupPrincipals(@NotNull String userId) throws Exception {
        if (syncConfig.user().getMembershipNestingDepth() == 1) {
            return ImmutableSet.copyOf(idp.getUser(userId).getDeclaredGroups()).stream().map(externalIdentityRef -> {
                try {
                    return new PrincipalImpl(idp.getIdentity(externalIdentityRef).getPrincipalName());
                } catch (ExternalIdentityException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toSet());
        } else {
            Set<Principal> set = new HashSet<>();
            collectExpectedPrincipals(set,
                    idp.getUser(userId).getDeclaredGroups(),
                    syncConfig.user().getMembershipNestingDepth());
            return set;
        }
    }

    private void collectExpectedPrincipals(Set<Principal> grPrincipals,
            @NotNull Iterable<ExternalIdentityRef> declaredGroups, long depth) throws Exception {
        if (depth <= 0) {
            return;
        }
        for (ExternalIdentityRef ref : declaredGroups) {
            ExternalIdentity ei = idp.getIdentity(ref);
            grPrincipals.add(new PrincipalImpl(ei.getPrincipalName()));
            collectExpectedPrincipals(grPrincipals, ei.getDeclaredGroups(), depth - 1);
        }
    }

    @Test
    public void testGetGroupMembershipExternalUserAndLocal() throws Exception {
        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(user);

        // same as in test before even if the principal is not a tree-based-principal
        Set<Principal> expected = getExternalGroupPrincipals(USER_ID);
        expected.add(testGroup.getPrincipal());
        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertEquals(expected.size(), principals.size());

        root.refresh(); //Refreshing root to make sure changes in cache are reflected
        Tree cacheTree = root.getTree(user.getPath()).getChild(REP_CACHE);
        assertNotNull(cacheTree);
        assertTrue(cacheTree.hasProperty(CACHE_PRINCIPAL_NAMES));
        assertFalse(cacheTree.getProperty(CACHE_PRINCIPAL_NAMES).getValue(Type.STRING).isEmpty());

        //Read again but this time from cache
        Set<Principal> readFromCache = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertEquals(expected, readFromCache);
    }

//    @Test
//    public void testCachedGroupPrincipalIsMember() throws Exception {
//        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
//        assertNotNull(user);
//        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(user.getPrincipal());
//        assertTrue(principals.contains(testGroup.getPrincipal()));
//
//        root.refresh(); //Refreshing root to make sure changes in cache are reflected
//        Tree cacheTree = root.getTree(user.getPath()).getChild(REP_CACHE);
//        assertNotNull(cacheTree);
//        assertTrue(cacheTree.hasProperty(CACHE_PRINCIPAL_NAMES));
//        assertFalse(cacheTree.getProperty(CACHE_PRINCIPAL_NAMES).getValue(Type.STRING).isEmpty());
//
//        Set<Principal> externalPrincipals = getExternalGroupPrincipals(USER_ID);
//        Set<Principal> cachedPrincipals = principalProvider.getMembershipPrincipals(user.getPrincipal());
//        cachedPrincipals.forEach(principal -> {
//            try {
//                assertTrue(principal instanceof ItemBasedPrincipal);
//                assertTrue(principal instanceof GroupPrincipal);
//                GroupPrincipal groupPrincipal = (GroupPrincipal) principal;
//                ItemBasedPrincipal itemBasedPrincipal = (ItemBasedPrincipal) principal;
//                assertNotNull(itemBasedPrincipal.getPath());
//                if (principal.getName().equals(testGroup.getPrincipal().getName())) {
//                    //Check if external group is a member of the cached group
//                    externalPrincipals.forEach(externalPrincipal -> {
//                        assertTrue(groupPrincipal.isMember(externalPrincipal));
//                    });
//
//                    var members = groupPrincipal.members();
//                    assertTrue(members.hasMoreElements());
//
//                }
//            } catch (RepositoryException e) {
//                throw new RuntimeException(e);
//            }
//        });
//    }
}
