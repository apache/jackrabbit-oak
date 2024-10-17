/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalGroupPrincipalProvider.CACHE_PRINCIPAL_NAMES;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class ExternalLoginCachedDynamicMembershipTest extends ExternalLoginTestBase {

    @Override
    public void before() throws Exception {
        super.before();

        syncConfig.user().setDynamicMembership(true);
        syncConfig.group().setDynamicGroups(true);

        // now register the sync-handler with the dynamic membership config
        // in order to enable dynamic membership with the external principal configuration
        Map<String, Boolean> props = ImmutableMap.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP,
                syncConfig.user().getDynamicMembership(),
                DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS,
                syncConfig.group().getDynamicGroups());
        SyncHandler syncHandler = WhiteboardUtils.getService(whiteboard, SyncHandler.class);
        context.registerService(SyncHandler.class, syncHandler, props);

        Map<String, String> mappingProps = ImmutableMap.of(
                SyncHandlerMapping.PARAM_IDP_NAME, idp.getName(),
                SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME, syncHandler.getName());
        context.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, mappingProps);
    }

    private Set<String> calcExpectedPrincipalNames(@NotNull ExternalIdentity identity, long depth) throws Exception {
        if (depth <= 0) {
            return Collections.emptySet();
        }
        Set<String> expected = new HashSet<>();
        for (ExternalIdentityRef ref : identity.getDeclaredGroups()) {
            ExternalIdentity groupIdentity = idp.getIdentity(ref);
            expected.add(groupIdentity.getPrincipalName());
            expected.addAll(calcExpectedPrincipalNames(groupIdentity, depth - 1));
        }
        return expected;
    }

    @Test
    public void testLoginPopulatesPrincipalCache() throws Exception {
        try (ContentSession cs = login(new SimpleCredentials(USER_ID, new char[0]))) {

            Set<String> expectedExternal = calcExpectedPrincipalNames(idp.getUser(USER_ID),
                    syncConfig.user().getMembershipNestingDepth());

            Set<Principal> principals = new HashSet<>(cs.getAuthInfo().getPrincipals());

            principals.forEach(principal -> expectedExternal.remove(principal.getName()));
            assertTrue(expectedExternal.isEmpty());

            root.refresh();

            UserManager uMgr = getUserManager(root);
            Authorizable authorizable = uMgr.getAuthorizable(USER_ID);

            Tree cache = root.getTree(authorizable.getPath()).getChild(CacheConstants.REP_CACHE);
            assertTrue(cache.exists());
            assertTrue(cache.hasProperty(CACHE_PRINCIPAL_NAMES));
            assertNotNull(cache.getProperty(CACHE_PRINCIPAL_NAMES).getValue(Type.STRING));
        }
    }

    @Test
    public void testLocalGroupAndUserIsCached() throws Exception {

        //Add external group to new local group
        Group groupToAddExternalGroup = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        String externalGroupName = idp.getGroup("a").getPrincipalName();
        assertTrue("Failed to add external group to local group", groupToAddExternalGroup.addMembers(externalGroupName).isEmpty());

        //Add user to new local group
        Group groupToAddUser = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        assertTrue("Failed to add user to local group", groupToAddUser.addMembers(USER_ID).isEmpty());

        root.commit();

        try (ContentSession cs = login(new SimpleCredentials(USER_ID, new char[0]))) {

            Set<Principal> principals = new HashSet<>(cs.getAuthInfo().getPrincipals());
            Set<String> expectedExternal = calcExpectedPrincipalNames(idp.getUser(USER_ID), syncConfig.user().getMembershipNestingDepth());
            principals.forEach(principal -> expectedExternal.remove(principal.getName()));
            assertTrue(expectedExternal.isEmpty());

            root.refresh();
            Authorizable authorizable = getUserManager(root).getAuthorizable(USER_ID);
            Tree cache = root.getTree(authorizable.getPath()).getChild(CacheConstants.REP_CACHE);
            assertTrue(cache.exists());
            assertTrue(cache.hasProperty(CACHE_PRINCIPAL_NAMES));

            // Check external group added to local group is cached
            String externalGroupCache = cache.getProperty(CACHE_PRINCIPAL_NAMES).getValue(Type.STRING);
            assertNotNull(externalGroupCache);
            assertTrue(externalGroupCache.contains(groupToAddExternalGroup.getID()));

            // Check external user added to local group is cached
            String externalUserCache = cache.getProperty("rep:groupPrincipalNames").getValue(Type.STRING);
            assertNotNull(externalUserCache);
            assertTrue(externalUserCache.contains(groupToAddUser.getID()));
        }
    }


    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        Map.of(CacheConstants.PARAM_CACHE_EXPIRATION, 10000,
                                CacheConstants.PARAM_CACHE_MAX_STALE, 10000,
                                //Configured to allow not yet created groups to be added as member of local groups
                                ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT
                        )
                ));
    }

    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                TokenLoginModule.class.getName(),
                                LoginModuleControlFlag.OPTIONAL,
                                Collections.emptyMap()),
                        new AppConfigurationEntry(
                                LoginModuleImpl.class.getName(),
                                LoginModuleControlFlag.OPTIONAL,
                                Collections.emptyMap()),
                        new AppConfigurationEntry(
                                ExternalLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)
                };
            }
        };
    }
}
