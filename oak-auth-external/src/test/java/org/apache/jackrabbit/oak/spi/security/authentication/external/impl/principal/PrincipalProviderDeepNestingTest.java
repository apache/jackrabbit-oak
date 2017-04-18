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

import java.security.Principal;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PrincipalProviderDeepNestingTest extends ExternalGroupPrincipalProviderTest {

    @Override
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig sc = super.createSyncConfig();
        sc.user().setMembershipNestingDepth(Long.MAX_VALUE);
        return sc;
    }

    @Override
    @Test
    public void testGetPrincipalDynamicGroup() throws Exception {
        for (ExternalIdentityRef ref : idp.getUser(USER_ID).getDeclaredGroups()) {

            String princName = idp.getIdentity(ref).getPrincipalName();
            Principal principal = principalProvider.getPrincipal(princName);

            assertNotNull(principal);
            assertTrue(principal instanceof java.security.acl.Group);
        }
    }

    @Override
    @Test
    public void testGetPrincipalInheritedGroups() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            ExternalIdentity externalGroup = idp.getIdentity(ref);
            Principal grPrincipal = principalProvider.getPrincipal(externalGroup.getPrincipalName());

            for (ExternalIdentityRef inheritedGroupRef : externalGroup.getDeclaredGroups()) {
                String inheritedPrincName = idp.getIdentity(inheritedGroupRef).getPrincipalName();

                Principal principal = principalProvider.getPrincipal(inheritedPrincName);

                assertNotNull(principal);
                assertTrue(principal instanceof java.security.acl.Group);

                java.security.acl.Group inheritedGrPrincipal = (java.security.acl.Group) principal;
                assertTrue(inheritedGrPrincipal.isMember(new PrincipalImpl(externalUser.getPrincipalName())));
                assertFalse(inheritedGrPrincipal.isMember(grPrincipal));
            }
        }
    }

    @Override
    @Test
    public void testFindPrincipalsByHintTypeGroup() throws Exception {
        Set<? extends Principal> expected = ImmutableSet.of(new PrincipalImpl("a"), new PrincipalImpl("aa"), new PrincipalImpl("aaa"));
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_GROUP));

        assertEquals(expected, res);
    }

    @Override
    @Test
    public void testFindPrincipalsByHintTypeAll() throws Exception {
        Set<? extends Principal> expected = ImmutableSet.of(
                new PrincipalImpl("a"),
                new PrincipalImpl("aa"),
                new PrincipalImpl("aaa"));
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_ALL));

        assertEquals(expected, res);
    }
}