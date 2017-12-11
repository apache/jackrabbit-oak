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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.SimpleCredentials;
import javax.jcr.ValueFactory;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ExternalLoginModuleAutoMembershipTest extends ExternalLoginModuleTestBase {

    private static final String NON_EXISTING_NAME = "nonExisting";

    private Root r;
    private UserManager userManager;
    private ValueFactory valueFactory;

    private ExternalSetup setup1;
    private ExternalSetup setup2;
    private ExternalSetup setup3;
    private ExternalSetup setup4;
    private ExternalSetup setup5;

    @Override
    public void before() throws Exception {
        super.before();

        r = getSystemRoot();
        userManager = getUserManager(r);
        valueFactory = getValueFactory(r);

        syncConfig.user().setDynamicMembership(true);

        // first configuration based on test base-setup with
        // - dynamic membership = true
        // - auto-membership = 'gr_default' and 'nonExisting'
        syncConfig.user().setDynamicMembership(true);
        setup1 = new ExternalSetup(idp, syncConfig, WhiteboardUtils.getService(whiteboard, SyncHandler.class), "gr" + UUID.randomUUID());

        // second configuration with different IDP ('idp2') and
        // - dynamic membership = true
        // - auto-membership = 'gr_name2' and 'nonExisting'
        DefaultSyncConfig sc2 = new DefaultSyncConfig();
        sc2.setName("name2").user().setDynamicMembership(true);
        setup2 = new ExternalSetup(new TestIdentityProvider("idp2"), sc2);

        // third configuration with different IDP  ('idp3') and
        // - dynamic membership = false
        // - auto-membership = 'gr_name3' and 'nonExisting'
        DefaultSyncConfig sc3 = new DefaultSyncConfig();
        sc3.setName("name3");
        setup3 = new ExternalSetup(new TestIdentityProvider("idp3"), sc3);

        // forth configuration based on different IDP ('idp4') but re-using
        // sync-handler configuration (sc2)
        setup4 = new ExternalSetup(new TestIdentityProvider("idp4"), sc2);

        // fifth configuration with different IDP ('idp5') and
        // - dynamic membership = true
        // - auto-membership => nothing configured
        DefaultSyncConfig sc5 = new DefaultSyncConfig();
        sc5.setName("name5").user().setDynamicMembership(true);
        setup5 = new ExternalSetup(new TestIdentityProvider("idp5"), sc5, new DefaultSyncHandler(sc5), null);
    }

    @Override
    public void after() throws Exception {
        try {
            syncConfig.user().setAutoMembership().setExpirationTime(0);

            setup1.close();
            setup2.close();
            setup3.close();
            setup4.close();
        } finally {
            super.after();
        }
    }

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry[] entries = new AppConfigurationEntry[5];
                int i = 0;
                for (ExternalSetup setup : new ExternalSetup[] {setup1, setup2, setup3, setup4, setup5}) {
                    entries[i++] = setup.asConfigurationEntry();
                }
                return entries;
            }
        };
    }

    private static void registerSyncHandlerMapping(@Nonnull OsgiContext ctx, @Nonnull ExternalSetup setup) {
        String syncHandlerName = setup.sc.getName();
        Map props = ImmutableMap.of(
                DefaultSyncConfigImpl.PARAM_NAME, syncHandlerName,
                DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, setup.sc.user().getDynamicMembership(),
                DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP, setup.sc.user().getAutoMembership());
        ctx.registerService(SyncHandler.class, setup.sh, props);

        Map mappingProps = ImmutableMap.of(
                SyncHandlerMapping.PARAM_IDP_NAME, setup.idp.getName(),
                SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME, syncHandlerName);
        ctx.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, mappingProps);
    }

    @Test
    public void testLoginSyncAutoMembershipSetup1() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, new char[0]));

            // the login must set the existing auto-membership principals to the subject
            Set<Principal> principals = cs.getAuthInfo().getPrincipals();
            assertTrue(principals.contains(setup1.gr.getPrincipal()));

            assertFalse(principals.contains(new PrincipalImpl(NON_EXISTING_NAME)));
            assertFalse(principals.contains(setup2.gr.getPrincipal()));
            assertFalse(principals.contains(setup3.gr.getPrincipal()));

            // however, the existing auto-membership group must _not_ have changed
            // and the test user must not be a stored member of this group.
            root.refresh();
            UserManager uMgr = getUserManager(root);

            User user = uMgr.getAuthorizable(USER_ID, User.class);
            Group gr = uMgr.getAuthorizable(setup1.gr.getID(), Group.class);

            assertFalse(gr.isDeclaredMember(user));
            assertFalse(gr.isMember(user));
        } finally {
            options.clear();
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testLoginAfterSyncSetup1() throws Exception {
        setup1.sync(USER_ID, false);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, new char[0]));

            // the login must set the configured + existing auto-membership principals
            // to the subject; non-existing auto-membership entries must be ignored.
            Set<Principal> principals = cs.getAuthInfo().getPrincipals();
            assertTrue(principals.contains(setup1.gr.getPrincipal()));

            assertFalse(principals.contains(new PrincipalImpl(NON_EXISTING_NAME)));
            assertFalse(principals.contains(setup2.gr.getPrincipal()));
            assertFalse(principals.contains(setup3.gr.getPrincipal()));

            // however, the existing auto-membership group must _not_ have changed
            // and the test user must not be a stored member of this group.
            root.refresh();
            UserManager uMgr = getUserManager(root);

            User user = uMgr.getAuthorizable(USER_ID, User.class);
            Group gr = uMgr.getAuthorizable(setup1.gr.getID(), Group.class);

            assertFalse(gr.isDeclaredMember(user));
            assertFalse(gr.isMember(user));
        } finally {
            options.clear();
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testLoginAfterSyncSetup2() throws Exception {
        setup2.sync(USER_ID, false);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, new char[0]));

            // the login must set the existing auto-membership principals to the subject
            Set<Principal> principals = cs.getAuthInfo().getPrincipals();
            assertTrue(principals.contains(setup2.gr.getPrincipal()));

            assertFalse(principals.contains(new PrincipalImpl(NON_EXISTING_NAME)));
            assertFalse(principals.contains(setup1.gr.getPrincipal()));
            assertFalse(principals.contains(setup3.gr.getPrincipal()));

            // however, the existing auto-membership group must _not_ have changed
            // and the test user must not be a stored member of this group.
            root.refresh();
            UserManager uMgr = getUserManager(root);

            User user = uMgr.getAuthorizable(USER_ID, User.class);
            Group gr = uMgr.getAuthorizable(setup2.gr.getID(), Group.class);

            assertFalse(gr.isDeclaredMember(user));
            assertFalse(gr.isMember(user));
        } finally {
            options.clear();
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testLoginAfterSyncSetup3() throws Exception {
        setup3.sync(USER_ID, false);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, new char[0]));

            // the login must set the existing auto-membership principals to the subject
            Set<Principal> principals = cs.getAuthInfo().getPrincipals();
            assertTrue(principals.contains(setup3.gr.getPrincipal()));

            assertFalse(principals.contains(new PrincipalImpl(NON_EXISTING_NAME)));
            assertFalse(principals.contains(setup1.gr.getPrincipal()));
            assertFalse(principals.contains(setup2.gr.getPrincipal()));

            // however, the existing auto-membership group must _not_ have changed
            // and the test user must not be a stored member of this group.
            root.refresh();
            UserManager uMgr = getUserManager(root);

            User user = uMgr.getAuthorizable(USER_ID, User.class);
            Group gr = uMgr.getAuthorizable(setup3.gr.getID(), Group.class);

            assertTrue(gr.isDeclaredMember(user));
            assertTrue(gr.isMember(user));
        } finally {
            options.clear();
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testLoginAfterSyncSetup4() throws Exception {
        setup4.sync(USER_ID, false);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, new char[0]));

            // the login must set the existing auto-membership principals to the subject
            Set<Principal> principals = cs.getAuthInfo().getPrincipals();
            assertTrue(principals.contains(setup4.gr.getPrincipal()));
            assertTrue(principals.contains(setup2.gr.getPrincipal()));

            assertFalse(principals.contains(new PrincipalImpl(NON_EXISTING_NAME)));
            assertFalse(principals.contains(setup1.gr.getPrincipal()));
            assertFalse(principals.contains(setup3.gr.getPrincipal()));

            // however, the existing auto-membership group must _not_ have changed
            // and the test user must not be a stored member of this group.
            root.refresh();
            UserManager uMgr = getUserManager(root);

            User user = uMgr.getAuthorizable(USER_ID, User.class);
            Group gr = uMgr.getAuthorizable(setup4.gr.getID(), Group.class);

            assertFalse(gr.isDeclaredMember(user));
            assertFalse(gr.isMember(user));
        } finally {
            options.clear();
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testLoginAfterSyncSetup5() throws Exception {
        setup5.sync(USER_ID, false);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, new char[0]));

            // the login must not set any auto-membership principals to the subject
            // as auto-membership is not configured on this setup.
            Set<Principal> principals = cs.getAuthInfo().getPrincipals();

            Set<Principal> expected = ImmutableSet.of(EveryonePrincipal.getInstance(), userManager.getAuthorizable(USER_ID).getPrincipal());
            assertEquals(expected, principals);

            assertFalse(principals.contains(new PrincipalImpl(NON_EXISTING_NAME)));
            assertFalse(principals.contains(setup1.gr.getPrincipal()));
            assertFalse(principals.contains(setup2.gr.getPrincipal()));
            assertFalse(principals.contains(setup3.gr.getPrincipal()));
            assertFalse(principals.contains(setup4.gr.getPrincipal()));
        } finally {
            options.clear();
            if (cs != null) {
                cs.close();
            }
        }
    }

    private final class ExternalSetup {

        private final ExternalIdentityProvider idp;
        private final Registration idpRegistration;

        private final DefaultSyncConfig sc;
        private final SyncHandler sh;
        private final Registration shRegistration;

        private final Group gr;

        private SyncContext ctx;

        private ExternalSetup(@Nonnull ExternalIdentityProvider idp, @Nonnull DefaultSyncConfig sc) throws Exception {
            this(idp, sc, new DefaultSyncHandler(sc), "gr_" + sc.getName());
        }

        private ExternalSetup(@Nonnull ExternalIdentityProvider idp, @Nonnull DefaultSyncConfig sc, @Nonnull SyncHandler sh, @CheckForNull String groupId) throws Exception {
            this.idp = idp;
            this.sc = sc;
            this.sh = sh;

            if (groupId != null) {
                Group g = userManager.getAuthorizable(groupId, Group.class);
                if (g != null) {
                    gr = g;
                } else {
                    gr = userManager.createGroup(groupId);
                }
                r.commit();

                sc.user().setAutoMembership(gr.getID(), NON_EXISTING_NAME).setExpirationTime(Long.MAX_VALUE);
            } else {
                gr = null;
            }

            idpRegistration = whiteboard.register(ExternalIdentityProvider.class, idp, Collections.<String, Object>emptyMap());
            shRegistration = whiteboard.register(SyncHandler.class, sh, ImmutableMap.of(
                            DefaultSyncConfigImpl.PARAM_NAME, sh.getName(),
                            DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, sc.user().getDynamicMembership(),
                            DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP, sc.user().getAutoMembership()));
            registerSyncHandlerMapping(context, this);
        }

        private void sync(@Nonnull String id, boolean isGroup) throws Exception {
            ctx = sh.createContext(idp, userManager, valueFactory);
            ExternalIdentity exIdentity = (isGroup) ? idp.getGroup(id) : idp.getUser(id);
            assertNotNull(exIdentity);

            SyncResult res = ctx.sync(exIdentity);
            assertEquals(idp.getName(), res.getIdentity().getExternalIdRef().getProviderName());
            assertSame(SyncResult.Status.ADD, res.getStatus());
            r.commit();
        }

        private void close() {
            if (ctx != null) {
                ctx.close();
            }
            if (idpRegistration != null) {
                idpRegistration.unregister();
            }
            if (shRegistration != null) {
                shRegistration.unregister();
            }
        }

        private AppConfigurationEntry asConfigurationEntry() {
            return new AppConfigurationEntry(
                    ExternalLoginModule.class.getName(),
                    AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                    ImmutableMap.<String, String>of(
                            SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME, sh.getName(),
                            SyncHandlerMapping.PARAM_IDP_NAME, idp.getName()
                    ));
        }
    }
}