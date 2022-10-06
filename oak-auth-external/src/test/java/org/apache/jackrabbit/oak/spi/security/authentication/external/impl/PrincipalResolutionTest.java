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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.PrincipalNameResolver;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_SECOND_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_TEST_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PrincipalResolutionTest extends DynamicSyncContextTest {

    @Override
    @NotNull
    protected ExternalIdentityProvider createIDP() {
        return new PrincipalResolvingIDP();
    }

    static class PrincipalResolvingIDP extends TestIdentityProvider implements PrincipalNameResolver {

        @NotNull
        @Override
        public String fromExternalIdentityRef(@NotNull ExternalIdentityRef externalIdentityRef) throws ExternalIdentityException {
            ExternalIdentity identity = getIdentity(externalIdentityRef);
            if (identity == null) {
                throw new ExternalIdentityException();
            } else {
                return identity.getPrincipalName();
            }
        }
    }

    @Test
    public void testSyncExternalUserGroupConflictPrincipalNameMismatch() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        ExternalIdentityRef ref = externalUser.getDeclaredGroups().iterator().next();
        ExternalIdentity externalGroup = idp.getIdentity(ref);
        assertNotNull(externalGroup);

        // create a local group that has the same ID but a mismatching principal name
        // since the shortcut is in place and dynamic groups are not synched the mismatch in principal-name
        // cannot be spotted (but also doesn't have too much implication apart from being confusing as principal names
        // are not case-insensitive and conflicting dynamic groups are not being created)
        assertSynched(externalUser, externalGroup, externalGroup.getId(), externalGroup.getPrincipalName()+"mismatch", ref);
    }

    @Test
    public void testSyncExternalUserGroupConflictPrincipalNameCaseMismatch() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        ExternalIdentityRef ref = externalUser.getDeclaredGroups().iterator().next();
        ExternalIdentity externalGroup = idp.getIdentity(ref);
        assertNotNull(externalGroup);

        // create a local group that has the same ID but a mismatching principal name
        // since the shortcut is in place and dynamic groups are not synched the mismatch in principal-name
        // cannot be spotted (but also doesn't have too much implication apart from being confusing as principal names
        // are not case-insensitive and conflicting dynamic groups are not being created)
        assertSynched(externalUser, externalGroup, externalGroup.getId(), externalGroup.getPrincipalName().toUpperCase(), ref);
    }

    private void assertSynched(@NotNull ExternalUser externalUser, @NotNull ExternalIdentity externalGroup,
                               @NotNull String existingId, @NotNull String existingPrincipalName, @Nullable ExternalIdentityRef existingGroupRef) throws Exception {

        Group g = userManager.createGroup(existingId, new PrincipalImpl(existingPrincipalName), null);
        if (existingGroupRef != null) {
            g.setProperty(REP_EXTERNAL_ID, getValueFactory().createValue(existingGroupRef.getString()));
        }
        r.commit();

        // sync the user with dynamic membership enabled
        sync(externalUser, SyncResult.Status.ADD);

        // retrieve rep:externalPrincipalNames
        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);

        // the resulting rep:externalPrincipalNames must contain the name of the principal
        Set<String> pNames = Sets.newHashSet(extPrincipalNames.getValue(Type.STRINGS));
        assertTrue(pNames + " must contain " + externalGroup.getPrincipalName(), pNames.contains(externalGroup.getPrincipalName()));
    }

    @Test
    public void testSyncMembershipWithUserRef() throws Exception {
        TestIdentityProvider.TestUser testuser = (TestIdentityProvider.TestUser) idp.getUser(ID_TEST_USER);
        Set<ExternalIdentityRef> groupRefs = getExpectedSyncedGroupRefs(syncConfig.user().getMembershipNestingDepth(), idp, testuser);

        // verify that the conflicting user has not been synced before
        assertNull(userManager.getAuthorizable(ID_SECOND_USER));
        
        ExternalUser second = idp.getUser(ID_SECOND_USER);
        testuser.withGroups(second.getExternalId());
        assertFalse(Iterables.elementsEqual(groupRefs, testuser.getDeclaredGroups()));

        sync(testuser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(ID_TEST_USER);
        assertTrue(a.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));

        // with IDP implementing PrincipalNameResolver the extra verification for all member-refs being groups 
        // is omitted _unless_ dynamic groups are enabled as well, in which case the short-cut is ignored.
        assertDynamicMembership(testuser, 1);
    }
}
