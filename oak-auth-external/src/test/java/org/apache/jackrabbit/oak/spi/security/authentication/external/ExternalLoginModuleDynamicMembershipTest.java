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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.security.Principal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExternalLoginModuleDynamicMembershipTest extends ExternalLoginModuleTest {

    @Override
    public void before() throws Exception {
        super.before();

        syncConfig.user().setDynamicMembership(true);

        // now register the sync-handler with the dynamic membership config
        // in order to enable dynamic membership with the external principal configuration
        Map props = ImmutableMap.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, syncConfig.user().getDynamicMembership());
        context.registerService(SyncHandler.class, WhiteboardUtils.getService(whiteboard, SyncHandler.class), props);
    }

    private void assertExternalPrincipalNames(@Nonnull UserManager userMgr, @Nonnull String id) throws Exception {
        Authorizable a = userMgr.getAuthorizable(id);
        assertNotNull(a);

        Set<String> expected = new HashSet();
        calcExpectedPrincipalNames(idp.getUser(id), syncConfig.user().getMembershipNestingDepth(), expected);

        Set<String> extPrincNames = new HashSet();
        for (Value v : a.getProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES)) {
            extPrincNames.add(v.getString());
        }

        assertEquals(expected, extPrincNames);
    }

    private void calcExpectedPrincipalNames(@Nonnull ExternalIdentity identity, long depth, @Nonnull Set<String> expected) throws Exception {
        if (depth <= 0) {
            return;
        }
        for (ExternalIdentityRef ref : identity.getDeclaredGroups()) {
            ExternalIdentity groupIdentity = idp.getIdentity(ref);
            expected.add(groupIdentity.getPrincipalName());
            calcExpectedPrincipalNames(groupIdentity, depth-1, expected);
        }
    }

    @Test
    public void testLoginPopulatesPrincipals() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, new char[0]));

            Set<String> expectedExternal = new HashSet<String>();
            calcExpectedPrincipalNames(idp.getUser(USER_ID), syncConfig.user().getMembershipNestingDepth(), expectedExternal);

            Set<Principal> principals = new HashSet<Principal>(cs.getAuthInfo().getPrincipals());

            root.refresh();
            PrincipalManager principalManager = getPrincipalManager(root);
            for (String pName : expectedExternal) {
                Principal p = principalManager.getPrincipal(pName);
                assertNotNull(p);
                assertTrue(principals.remove(p));
            }

            UserManager uMgr = getUserManager(root);
            User u = uMgr.getAuthorizable(USER_ID, User.class);
            assertTrue(principals.remove(u.getPrincipal()));

            Iterator<Group> it = u.memberOf();
            assertFalse(it.hasNext());

            assertTrue(principals.remove(EveryonePrincipal.getInstance()));
            assertTrue(principals.isEmpty());

        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncCreatesRepExternalPrincipals() throws Exception {
        try {
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            root.refresh();
            assertExternalPrincipalNames(getUserManager(root), USER_ID);
        } finally {
            options.clear();
        }
    }

    @Test
    public void testSyncCreatesRepExternalPrincipalsDepthInfinite() throws Exception {
        syncConfig.user().setMembershipNestingDepth(Long.MAX_VALUE);
        try {
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            root.refresh();
            assertExternalPrincipalNames(getUserManager(root), USER_ID);
        } finally {
            options.clear();
        }
    }

    @Test
    public void testSyncCreateGroup() throws Exception {
        try {
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            root.refresh();
            UserManager userManager = getUserManager(root);
            for (String id : new String[]{"a", "b", "c"}) {
                assertNull(userManager.getAuthorizable(id));
            }
            for (String id : new String[]{"aa", "aaa"}) {
                assertNull(userManager.getAuthorizable(id));
            }
        } finally {
            options.clear();
        }
    }

    @Test
    public void testSyncCreateGroupNesting() throws Exception {
        syncConfig.user().setMembershipNestingDepth(2);
        try {
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            root.refresh();
            for (String id : new String[]{"a", "b", "c", "aa", "aaa"}) {
                assertNull(getUserManager(root).getAuthorizable(id));
            }
        } finally {
            options.clear();
        }
    }

    @Test
    public void testSyncUpdateAfterXmlImport() throws Exception {
        try {
            // force initial sync
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            // remove properties according to the behavior in the XML-import
            Root systemRoot = getSystemRoot();
            UserManager userManager = getUserManager(systemRoot);

            Authorizable a = userManager.getAuthorizable(USER_ID);
            a.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
            a.removeProperty(ExternalIdentityConstants.REP_LAST_SYNCED);
            systemRoot.commit();

            // login again to force sync of the user (and it's group membership)
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            systemRoot.refresh();
            a = userManager.getAuthorizable(USER_ID);
            assertTrue(a.hasProperty(ExternalIdentityConstants.REP_LAST_SYNCED));
            assertExternalPrincipalNames(userManager, USER_ID);
        } finally {
            options.clear();
        }
    }

    @Test
    public void testSyncUpdateWithRemovedPrincipalNames() throws Exception {
        try {
            // force initial sync
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            // removing the rep:externalPrincipalNames property only will have the same
            // effect as the compatibility behavior that respects previously
            // synchronized users with full membership sync.
            Root systemRoot = getSystemRoot();
            UserManager userManager = getUserManager(systemRoot);
            User user = userManager.getAuthorizable(USER_ID, User.class);
            user.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
            systemRoot.commit();

            waitUntilExpired(user, systemRoot, syncConfig.user().getExpirationTime());

            // login again
            login(new SimpleCredentials(USER_ID, new char[0])).close();

            systemRoot.refresh();
            user = userManager.getAuthorizable(USER_ID, User.class);
            assertTrue(user.hasProperty(ExternalIdentityConstants.REP_LAST_SYNCED));
            assertFalse(user.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES));

            for (ExternalIdentityRef ref : idp.getUser(USER_ID).getDeclaredGroups()) {
                assertNotNull(userManager.getAuthorizable(ref.getId()));
            }
        } finally {
            options.clear();
        }
    }
}