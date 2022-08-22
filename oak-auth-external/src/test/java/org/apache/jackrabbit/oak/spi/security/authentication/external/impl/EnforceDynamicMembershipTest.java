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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class EnforceDynamicMembershipTest extends DynamicSyncContextTest {

    @Override
    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig sc = super.createSyncConfig();
        sc.user().setDynamicMembership(true).setEnforceDynamicMembership(true);
        return sc;
    }

    @Test
    public void testSyncMembershipWithChangedExistingGroups() throws Exception {
        Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);
        
        // set with different groups than defined on IDP
        TestUserWithGroupRefs mod = new TestUserWithGroupRefs(previouslySyncedUser, ImmutableSet.of(
                idp.getGroup("a").getExternalId(),
                idp.getGroup("aa").getExternalId(),
                idp.getGroup("secondGroup").getExternalId()));
        syncContext.syncMembership(mod, a, 1);

        Tree t = r.getTree(a.getPath());
        assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertMigratedGroups(userManager, mod, null);
        assertMigratedGroups(userManager, previouslySyncedUser, null);
    }

    @Test
    public void testSyncExternalUserExistingGroups() throws Exception {
        // add an addition member to one group
        ExternalIdentityRef grRef = previouslySyncedUser.getDeclaredGroups().iterator().next();
        Group gr = userManager.getAuthorizable(grRef.getId(), Group.class);
        gr.addMember(userManager.createGroup("someOtherMember"));
        r.commit();
        
        syncConfig.user().setMembershipExpirationTime(-1);
        // create a new context to make sure the membership data has expired
        DynamicSyncContext dsc = new DynamicSyncContext(syncConfig, idp, userManager, valueFactory);
        dsc.setForceUserSync(true);
        assertSame(SyncResult.Status.UPDATE, dsc.sync(previouslySyncedUser).getStatus());
        
        // membership must have been migrated from group to rep:externalPrincipalNames
        // groups that have no other members left must be deleted.
        Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);
        assertNotNull(a);
        Tree t = r.getTree(a.getPath());
        assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertDynamicMembership(previouslySyncedUser, 1);
        assertMigratedGroups(userManager, previouslySyncedUser, grRef);
    }

    @Test
    public void testGroupFromDifferentIDP() throws Exception {
        Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);
        // add as member to a group from a different IDP
        Group gr = userManager.createGroup("anotherGroup");
        gr.addMember(a);
        r.commit();

        syncConfig.user().setMembershipExpirationTime(-1);
        // create a new context to make sure the membership data has expired
        DynamicSyncContext dsc = new DynamicSyncContext(syncConfig, idp, userManager, valueFactory);
        dsc.setForceUserSync(true);
        assertSame(SyncResult.Status.UPDATE, dsc.sync(previouslySyncedUser).getStatus());       
        r.commit();
        
        // membership must have been migrated from group to rep:externalPrincipalNames
        // groups that have no other members left must be deleted.
        Tree t = r.getTree(a.getPath());
        assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertDynamicMembership(previouslySyncedUser, 1);
        assertMigratedGroups(userManager, previouslySyncedUser, null);
        
        gr = userManager.getAuthorizable("anotherGroup", Group.class);
        assertNotNull(gr);
        assertTrue(gr.isMember(a));
    }

    private static void assertMigratedGroups(@NotNull UserManager userManager,
                                             @NotNull ExternalIdentity externalIdentity, 
                                             @Nullable ExternalIdentityRef grRef) throws Exception {
        for (ExternalIdentityRef ref : externalIdentity.getDeclaredGroups()) {
            Group gr = userManager.getAuthorizable(ref.getId(), Group.class);
            if (ref.equals(grRef)) {
                assertNotNull(gr);
            } else {
                assertNull(gr);
            }
        }
    }
}