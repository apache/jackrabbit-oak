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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.ValueFactory;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public @RunWith(Parameterized.class)
class AutoMembershipTest extends AbstractExternalAuthTest {

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[]{true, "DynamicSync=true"},
                new Object[]{false, "DynamicSync=false"});
    }

    private final boolean dynamicSync;
    private Root r;
    private UserManager userManager;
    private Group groupAutomembership;
    private Group userAutomembership;
    private User externalUser;
    private Group externalGroup;
    private Group testGroup;
    
    public AutoMembershipTest(boolean dynamicSync, @NotNull String name) {
        this.dynamicSync = dynamicSync;
    }

    @Before
    public void before() throws Exception {
        super.before();

        // inject user-configuration as well as sync-handler and sync-hander-mapping to have get dynamic-membership 
        // providers registered.
        context.registerInjectActivateService(getUserConfiguration());
        registerSyncHandler(syncConfigAsMap(), idp.getName());
        
        r = getSystemRoot();
        userManager = getUserManager(r);

        // create automembership groups
        groupAutomembership = userManager.createGroup("groupAutomembership");
        userAutomembership = userManager.createGroup("userAutomembership1");
        
        Group groupInherited = userManager.createGroup("groupInherited");
        groupInherited.addMembers("groupAutomembership", "userAutomembership");
        
        TestIdentityProvider tidp = (TestIdentityProvider) idp;
        tidp.addUser(new TestIdentityProvider.TestUser("externalUser", idp.getName()));
        tidp.addGroup(new TestIdentityProvider.TestGroup("externalGroup", idp.getName()));

        ValueFactory valueFactory = getValueFactory(r);
        SyncContext syncCtx = (dynamicSync) ?
                new DynamicSyncContext(syncConfig, idp, userManager, valueFactory) :
                new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);

        assertEquals(SyncResult.Status.ADD, syncCtx.sync(idp.getUser("externalUser")).getStatus());
        assertEquals(SyncResult.Status.ADD, syncCtx.sync(idp.getGroup("externalGroup")).getStatus());
        r.commit();
        
        externalUser = userManager.getAuthorizable("externalUser", User.class);
        externalGroup = userManager.getAuthorizable("externalGroup", Group.class);
        assertNotNull(externalUser);
        assertNotNull(externalGroup);
    }

    @Override
    public void after() throws Exception {
        try {
            if (externalUser != null) {
                externalUser.remove();
            }
            if (externalGroup != null) {
                externalGroup.remove();
            }
            if (testGroup != null) {
                testGroup.remove();
            }
            r.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.user().setDynamicMembership(dynamicSync);
        config.group().setDynamicGroups(dynamicSync);
        config.group().setAutoMembership("groupAutomembership");
        config.user().setAutoMembership("userAutomembership1","userAutomembership2");
        return config;
    }

    private Group getTestGroup(@NotNull Authorizable... members) throws Exception {
        if (testGroup == null) {
            testGroup = userManager.createGroup("testGroup");
        }
        for (Authorizable member : members) {
            testGroup.addMember(member);
        }
        r.commit();
        return testGroup;
    }
    
    @Test
    public void testIsDeclaredMemberConfiguredUserAutoMembership() throws Exception {
        assertFalse(userAutomembership.isDeclaredMember(getTestUser()));
        assertFalse(userAutomembership.isDeclaredMember(getTestGroup()));
        assertFalse(userAutomembership.isDeclaredMember(externalGroup));

        assertTrue(userAutomembership.isDeclaredMember(externalUser));
    }

    @Test
    public void testIsDeclaredMemberConfiguredGroupAutoMembership() throws Exception {
        assertFalse(groupAutomembership.isDeclaredMember(getTestUser()));
        assertFalse(groupAutomembership.isDeclaredMember(getTestGroup()));

        assertTrue(groupAutomembership.isDeclaredMember(externalGroup));
        // dynamic automembership for users also includes the configured group-automembership (to account for cases where dynamic-group option is false)
        assertEquals(dynamicSync, groupAutomembership.isDeclaredMember(externalUser));
    }

    @Test
    public void testIsMemberConfiguredUserAutoMembership() throws Exception {
        assertFalse(userAutomembership.isMember(getTestUser()));
        assertFalse(userAutomembership.isMember(getTestGroup()));
        assertFalse(userAutomembership.isMember(externalGroup));

        assertTrue(userAutomembership.isMember(externalUser));
    }

    @Test
    public void testIsMemberConfiguredGroupAutoMembership() throws Exception {
        assertFalse(groupAutomembership.isMember(getTestUser()));
        assertFalse(groupAutomembership.isMember(getTestGroup()));

        assertTrue(groupAutomembership.isMember(externalGroup));
        // dynamic automembership for users also includes the configured group-automembership (to account for cases where dynamic-group option is false)
        assertEquals(dynamicSync, groupAutomembership.isMember(externalUser));
    }

    @Test
    public void testIsMemberNestedGroup() throws Exception {
        // automembership groups are members of other groups
        User testuser = getTestUser();
        Group nested = getTestGroup(userAutomembership, groupAutomembership, testuser);
        r.commit();

        // test nested group
        assertTrue(nested.isMember(testuser));
        assertTrue(nested.isMember(userAutomembership));
        assertTrue(nested.isMember(groupAutomembership));
        assertTrue(nested.isMember(externalUser));
        assertTrue(nested.isMember(externalGroup));

        // user-automembership-group
        assertFalse(userAutomembership.isMember(nested));
        assertFalse(userAutomembership.isMember(testuser));
        assertFalse(userAutomembership.isMember(groupAutomembership));
        assertFalse(userAutomembership.isMember(externalGroup));
        assertTrue(userAutomembership.isMember(externalUser));

        // group-automembership-group
        assertFalse(groupAutomembership.isMember(nested));
        assertFalse(groupAutomembership.isMember(testuser));
        assertFalse(groupAutomembership.isMember(userAutomembership));
        assertTrue(groupAutomembership.isMember(externalGroup));
        // dynamic automembership for users also includes the configured group-automembership (to account for cases where dynamic-group option is false)
        assertEquals(dynamicSync, groupAutomembership.isMember(externalUser));
    }

    @Test
    public void testIsMemberNestedGroupInverse() throws Exception {
        User testuser = getTestUser();
        Group nested = getTestGroup(testuser);
        userAutomembership.addMember(nested);
        groupAutomembership.addMember(nested);
        r.commit();

        // test nested group
        assertTrue(nested.isMember(testuser));
        assertFalse(nested.isMember(userAutomembership));
        assertFalse(nested.isMember(groupAutomembership));
        assertFalse(nested.isMember(externalUser));
        assertFalse(nested.isMember(externalGroup));

        // user-automembership-group
        assertTrue(userAutomembership.isMember(nested));
        assertTrue(userAutomembership.isMember(testuser));
        assertFalse(userAutomembership.isMember(groupAutomembership));
        assertFalse(userAutomembership.isMember(externalGroup));
        assertTrue(userAutomembership.isMember(externalUser));

        // group-automembership-group
        assertTrue(groupAutomembership.isMember(nested));
        assertTrue(groupAutomembership.isMember(testuser));
        assertFalse(groupAutomembership.isMember(userAutomembership));
        assertTrue(groupAutomembership.isMember(externalGroup));
        // dynamic automembership for users also includes the configured group-automembership (to account for cases where dynamic-group option is false)
        assertEquals(dynamicSync, groupAutomembership.isMember(externalUser));
    }

    @Test
    public void testIsMemberExternalUserInheritedNested() throws Exception {
        Group testGroup = getTestGroup();
        Group base = userManager.createGroup("baseGroup");
        base.addMember(testGroup);
        r.commit();

        assertFalse(base.isDeclaredMember(externalUser));
        assertFalse(base.isMember(externalUser));

        // add 'automembership-group' as nested members
        testGroup.addMember(userAutomembership);
        r.commit();
        
        assertFalse(base.isDeclaredMember(externalUser));
        assertTrue(base.isMember(externalUser));
    }
}