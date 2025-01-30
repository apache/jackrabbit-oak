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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.collections.StreamUtils;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;

import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import java.security.Principal;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertSame;

public abstract class AbstractDynamicTest extends AbstractExternalAuthTest {
    
    Root r;
    UserManager userManager;
    ValueFactory valueFactory;

    DynamicSyncContext syncContext;

    // the external user identity that has been synchronized before dynamic membership is enabled.
    ExternalUser previouslySyncedUser;

    @Before
    public void before() throws Exception {
        super.before();
        r = getSystemRoot();

        createAutoMembershipGroups();
        previouslySyncedUser = syncPriorToDynamicMembership();
        long now = System.currentTimeMillis();

        userManager = getUserManager(r);
        valueFactory = getValueFactory(r);

        while (now == System.currentTimeMillis()) {
            // Wait to ensure that the internal timestamp of the DynamicSyncContext is ahead of the last sync time (OAK-10379)
        }

        syncContext = new DynamicSyncContext(syncConfig, idp, userManager, valueFactory);

        // inject user-configuration as well as sync-handler and sync-hander-mapping to have get dynamic-membership 
        // providers registered.
        context.registerInjectActivateService(getUserConfiguration());
        registerSyncHandler(syncConfigAsMap(), idp.getName());
    }

    @After
    public void after() throws Exception {
        try {
            syncContext.close();
            r.refresh();
        } finally {
            super.after();
        }
    }

    private void createAutoMembershipGroups() throws RepositoryException {
        DefaultSyncConfig sc = createSyncConfig();
        UserManager um = getUserManager(r);
        // create automembership groups
        for (String id : Iterables.concat(sc.user().getAutoMembership(), sc.group().getAutoMembership())) {
            um.createGroup(id);
        }
    }

    /**
     * Synchronized a separate user with DefaultSyncContext to test behavior for previously synchronized user/group
     * with deep membership-nesting => all groups synched
     */
    @NotNull
    abstract ExternalUser syncPriorToDynamicMembership() throws Exception;

    @Override
    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig sc = super.createSyncConfig();
        sc.user().setDynamicMembership(true);
        return sc;
    }

    protected void sync(@NotNull ExternalIdentity externalIdentity, @NotNull SyncResult.Status expectedStatus) throws Exception {
        SyncResult result = syncContext.sync(externalIdentity);
        assertSame(expectedStatus, result.getStatus());
        r.commit();
    }

    @NotNull
    static List<String> getIds(@NotNull Iterator<? extends Authorizable> authorizables) {
        return StreamUtils.toStream(authorizables).map(authorizable -> {
            try {
                return authorizable.getID();
            } catch (RepositoryException repositoryException) {
                throw new RuntimeException();
            }
        }).collect(Collectors.toList());
    }

    @NotNull
    static List<String> getPrincipalNames(@NotNull Iterator<Principal> groupPrincipals) {
        return StreamUtils.toStream(groupPrincipals).map(Principal::getName).collect(Collectors.toList());
    }
}