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

import java.util.Calendar;

import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * DefaultSyncHandlerTest
 */
public class DefaultSyncHandlerTest extends ExternalLoginModuleTestBase {

    private final String userId = "testUser";

    @Before
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    protected ExternalIdentityProvider createIDP() {
        return new TestIdentityProvider();
    }

    @Override
    protected void destroyIDP(ExternalIdentityProvider idp) {
    // ignore
    }

    @Override
    protected void setSyncConfig(DefaultSyncConfig cfg) {
        if (cfg != null) {
            cfg.user().setExpirationTime(500);
        }
        super.setSyncConfig(cfg);
    }

    @Test
    public void testFindMissingIdentity() throws Exception {
        UserManager userManager = getUserManager(root);
        SyncHandler mgr = syncManager.getSyncHandler("default");
        SyncedIdentity id = mgr.findIdentity(userManager, "foobar");
        assertNull("unknown authorizable should not exist", id);
    }

    @Test
    public void testFindLocalIdentity() throws Exception {
        UserManager userManager = getUserManager(root);
        SyncHandler mgr = syncManager.getSyncHandler("default");
        SyncedIdentity id = mgr.findIdentity(userManager, "admin");
        assertNotNull("known authorizable should exist", id);
        assertNull("local user should not have external ref", id.getExternalIdRef());
    }

    @Test
    public void testFindExternalIdentity() throws Exception {
        login(new SimpleCredentials(userId, new char[0])).close();
        root.refresh();

        UserManager userManager = getUserManager(root);
        SyncHandler mgr = syncManager.getSyncHandler("default");
        SyncedIdentity id = mgr.findIdentity(userManager, userId);
        assertNotNull("known authorizable should exist", id);
        assertEquals("external user should have correct external ref.idp", idp.getName(), id.getExternalIdRef().getProviderName());
        assertEquals("external user should have correct external ref.id", userId, id.getExternalIdRef().getId());
    }

    @Test
    public void testRequiresNoSync() throws Exception {
        login(new SimpleCredentials(userId, new char[0])).close();
        root.refresh();

        UserManager userManager = getUserManager(root);
        SyncHandler mgr = syncManager.getSyncHandler("default");
        SyncedIdentity id = mgr.findIdentity(userManager, userId);
        assertNotNull("known authorizable should exist", id);

        assertFalse("freshly synced id should not require sync", mgr.requiresSync(id));
    }

    @Test
    public void testRequiresSync() throws Exception {
        login(new SimpleCredentials(userId, new char[0])).close();
        root.refresh();

        ValueFactory valueFactory = new ValueFactoryImpl(root, NamePathMapper.DEFAULT);
        final Calendar nowCal = Calendar.getInstance();
        nowCal.setTimeInMillis(nowCal.getTimeInMillis() - 1000);
        Value nowValue = valueFactory.createValue(nowCal);

        UserManager userManager = getUserManager(root);
        Authorizable a = userManager.getAuthorizable(userId);
        a.setProperty(DefaultSyncContext.REP_LAST_SYNCED, nowValue);
        root.commit();

        SyncHandler mgr = syncManager.getSyncHandler("default");
        SyncedIdentity id = mgr.findIdentity(userManager, userId);
        assertNotNull("known authorizable should exist", id);

        assertTrue("synced id should require sync", mgr.requiresSync(id));
    }


}