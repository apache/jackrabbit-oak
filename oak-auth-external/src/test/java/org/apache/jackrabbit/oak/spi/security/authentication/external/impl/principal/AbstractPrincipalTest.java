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
import java.util.UUID;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DynamicSyncContext;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPrincipalTest extends AbstractExternalAuthTest {

    PrincipalProvider principalProvider;

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
        principalProvider = createPrincipalProvider();
    }

    @Nonnull
    PrincipalProvider createPrincipalProvider() {
        Set<String> autoMembership = ImmutableSet.copyOf(Iterables.concat(syncConfig.user().getAutoMembership(),syncConfig.group().getAutoMembership()));
        return new ExternalGroupPrincipalProvider(root, getSecurityProvider().getConfiguration(UserConfiguration.class),
                NamePathMapper.DEFAULT, ImmutableMap.of(idp.getName(), autoMembership.toArray(new String[autoMembership.size()])));
    }

    @Override
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        DefaultSyncConfig.User u = config.user();
        u.setDynamicMembership(true);
        return config;
    }

    java.security.acl.Group getGroupPrincipal() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        return getGroupPrincipal(externalUser.getDeclaredGroups().iterator().next());
    }

    java.security.acl.Group getGroupPrincipal(@Nonnull ExternalIdentityRef ref) throws Exception {
        String principalName = idp.getIdentity(ref).getPrincipalName();
        Principal p = principalProvider.getPrincipal(principalName);

        assertNotNull(p);
        assertTrue(p instanceof java.security.acl.Group);

        return (java.security.acl.Group) p;
    }

    Group createTestGroup() throws Exception {
        Group gr = getUserManager(root).createGroup("group" + UUID.randomUUID());
        root.commit();
        return gr;
    }
}