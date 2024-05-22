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

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.Value;
import javax.jcr.ValueFactory;

import java.util.Calendar;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_LAST_DYNAMIC_SYNC;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_LAST_SYNCED;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_MEMBERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SwitchSyncModeTest extends AbstractDynamicTest {

    @NotNull ExternalUser syncPriorToDynamicMembership() throws Exception {
        return syncWithDefaultContext(SyncResult.Status.ADD);
    }
    
    private @NotNull ExternalUser syncWithDefaultContext(@NotNull SyncResult.Status expectedStatus) throws Exception {
        DefaultSyncContext defaultSyncContext = createDefaultSyncContext(r);
        ExternalUser previouslySyncedUser = idp.getUser(USER_ID);
        assertNotNull(previouslySyncedUser);

        SyncResult result = defaultSyncContext.sync(previouslySyncedUser);
        assertSame(expectedStatus, result.getStatus());
        defaultSyncContext.close();
        r.commit();

        return previouslySyncedUser;
    }
    
    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.user().setDynamicMembership(true)
                .setEnforceDynamicMembership(true)
                .setMembershipExpirationTime(1)
                .setExpirationTime(1);
        config.group().setDynamicGroups(true)
                .setExpirationTime(1);
        return config;
    }
    
    private @NotNull DefaultSyncContext createDefaultSyncContext(@NotNull Root r) {
        UserManager um = getUserManager(r);
        return new DefaultSyncContext(createNonDynamicConfig(), idp, um, getValueFactory(r));
    }
    
    private @NotNull DefaultSyncConfig createNonDynamicConfig() {
        DefaultSyncConfig config = createSyncConfig();
        config.user().setDynamicMembership(false)
                .setEnforceDynamicMembership(false)
                .setMembershipNestingDepth(Long.MAX_VALUE)
                .setMembershipExpirationTime(1);
        config.group().setDynamicGroups(false);
        return config;
    }
    
    private static void assertIsDynamic(@NotNull ExternalUser externalUser, @NotNull UserManager um, @NotNull Root root, boolean expectedDynamic) throws Exception {
        User user = um.getAuthorizable(externalUser.getId(), User.class);
        assertNotNull(user);
        assertEquals(expectedDynamic, user.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertEquals(expectedDynamic, user.hasProperty(REP_LAST_DYNAMIC_SYNC));
        assertTrue(user.hasProperty(REP_LAST_SYNCED));
        
        assertGroups(externalUser, um, root, !expectedDynamic);
    }
    
    private static void assertGroups(@NotNull ExternalUser externalUser, @NotNull UserManager um, @NotNull Root r,  boolean expectedMemberReference) throws Exception {
        for (ExternalIdentityRef groupRef : externalUser.getDeclaredGroups()) {
            Group gr = um.getAuthorizable(groupRef.getId(), Group.class);
            assertNotNull(gr);
            
            Tree t = r.getTree(gr.getPath());
            assertEquals(expectedMemberReference, t.hasProperty(REP_MEMBERS));
        }
    }

    @Test
    public void testDefaultDynamic() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        assertNotNull(externalUser);
        
        // assert previously synched user is not dynamic
        assertIsDynamic(externalUser, getUserManager(r), r, false);

        // assert previously synched user is now dynamic
        sync(externalUser, SyncResult.Status.UPDATE);
        assertIsDynamic(externalUser, getUserManager(r), r, true);
    }

    @Test
    public void testDefaultMissingCleanupDynamic() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        assertNotNull(externalUser);
        
        // modified the default-synchronized user to mock the issue reported in OAK-10517 
        UserManager um = getUserManager(r);
        User user = um.getAuthorizable(externalUser.getId(), User.class);
        user.setProperty(REP_EXTERNAL_PRINCIPAL_NAMES, new Value[] {getValueFactory(r).createValue("test-values")});
        r.commit();
        
        // assert previously synched user is now dynamic and groups have been cleaned
        sync(externalUser, SyncResult.Status.UPDATE);
        assertIsDynamic(externalUser, getUserManager(r), r, true);
    }

    @Test
    public void testDefaultSyncClearsDynamicProperties() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        assertNotNull(externalUser);

        assertIsDynamic(externalUser, getUserManager(r), r, false);

        // modified the default-synchronized user to mock the issue reported in OAK-10517 
        UserManager um = getUserManager(r);
        User user = um.getAuthorizable(externalUser.getId(), User.class);
        ValueFactory vf = getValueFactory(r);
        user.setProperty(REP_EXTERNAL_PRINCIPAL_NAMES, new Value[] {vf.createValue("test-values")});
        user.setProperty(REP_LAST_DYNAMIC_SYNC, vf.createValue(Calendar.getInstance()));
        r.commit();

        // verify that synchronizing with the default-context again clears all properties that would have been set
        // if dynamic-sync had been enabled temporarily in between.
        syncWithDefaultContext(SyncResult.Status.UPDATE);
        assertIsDynamic(externalUser, getUserManager(r), r, false);
    }
}