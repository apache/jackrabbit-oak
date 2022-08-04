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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DynamicSyncContext;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPrincipalTest extends AbstractExternalAuthTest {

    ExternalGroupPrincipalProvider principalProvider;

    @Override
    public void before() throws Exception {
        super.before();

        // sync external users into the system using the 2 different sync-context implementations
        Root systemRoot = getSystemRoot();
        SyncContext syncContext = new DynamicSyncContext(syncConfig, idp, getUserManager(systemRoot), getValueFactory(systemRoot));
        syncContext.sync(idp.getUser(USER_ID));
        syncContext.close();

        syncContext = new DefaultSyncContext(syncConfig, idp, getUserManager(systemRoot), getValueFactory(systemRoot));
        syncContext.sync(idp.getUser(TestIdentityProvider.ID_SECOND_USER));
        syncContext.close();

        systemRoot.commit();

        root.refresh();
        principalProvider = createPrincipalProvider(root, getSecurityProvider().getConfiguration(UserConfiguration.class));
    }

    @NotNull
    ExternalGroupPrincipalProvider createPrincipalProvider(@NotNull Root r, @NotNull UserConfiguration uc) {
        Set<String> idpNamesWithDynamicGroups = getIdpNamesWithDynamicGroups();
        boolean hasOnlyDynamicGroups = (idpNamesWithDynamicGroups.size() == 1 && idpNamesWithDynamicGroups.contains(idp.getName()));
        return new ExternalGroupPrincipalProvider(r, uc, getNamePathMapper(), idp.getName(), syncConfig, idpNamesWithDynamicGroups, hasOnlyDynamicGroups);    
    }

    @Override
    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        DefaultSyncConfig.User u = config.user();
        u.setDynamicMembership(true);
        return config;
    }

    @NotNull String[] getAutoMembership() {
        return Iterables.toArray(Iterables.concat(syncConfig.user().getAutoMembership(),syncConfig.group().getAutoMembership()), String.class);
    }
    
    @NotNull AutoMembershipConfig getAutoMembershipConfig() {
        return AutoMembershipConfig.EMPTY;
    }
    
    @NotNull Set<String> getIdpNamesWithDynamicGroups() {
        return Collections.emptySet();
    }

    @NotNull GroupPrincipal getGroupPrincipal() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        return getGroupPrincipal(externalUser.getDeclaredGroups().iterator().next());
    }

    @NotNull GroupPrincipal getGroupPrincipal(@NotNull ExternalIdentityRef ref) throws Exception {
        String principalName = idp.getIdentity(ref).getPrincipalName();
        Principal p = principalProvider.getPrincipal(principalName);

        assertNotNull(p);
        assertTrue(p instanceof GroupPrincipal);

        return (GroupPrincipal) p;
    }

    @NotNull Group createTestGroup() throws Exception {
        return createTestGroup(root);
    }

    @NotNull Group createTestGroup(@NotNull Root r) throws Exception {
        Group gr = getUserManager(r).createGroup("group" + UUID.randomUUID());
        r.commit();
        return gr;
    }
}
