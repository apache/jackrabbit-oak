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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * ExternalLoginModuleTest...
 */
public class ExternalLoginModuleTest extends AbstractSecurityTest {

    private final HashMap<String, Object> options = new HashMap<String, Object>();

    private String userId;
    private Set<String> ids = new HashSet<String>();

    private UserManager userManager;

    private Root root;

    @Before
    public void before() throws Exception {
        super.before();

        userId = TestLoginModule.externalUser.getId();
        ids.add(userId);
        for (ExternalGroup group : TestLoginModule.externalGroups) {
            ids.add(group.getId());
        }

        root = admin.getLatestRoot();
        userManager = securityProvider.getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);
    }

    @After
    public void after() throws Exception {
        try {
            for (String id : ids) {
                Authorizable a = userManager.getAuthorizable(id);
                if (a != null) {
                    a.remove();
                }
            }
            root.commit();
        } finally {
            root.refresh();
            super.after();
        }
    }



    @Test
    public void testLoginFailed() throws Exception {
       try {
           ContentSession cs = login(new SimpleCredentials("unknown", new char[0]));
           cs.close();
           fail("login failure expected");
       } catch (LoginException e) {
           // success
       } finally {
           assertNull(userManager.getAuthorizable(userId));
       }
    }

    @Test
    public void testSyncCreateUser() throws Exception {
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, SyncMode.CREATE_USER);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : ids) {
                if (id.equals(userId)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    assertNotNull(a);
                    for (String prop : TestLoginModule.externalUser.getProperties().keySet()) {
                        assertTrue(a.hasProperty(prop));
                    }
                } else {
                    assertNull(userManager.getAuthorizable(id));
                }
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncCreateGroup() throws Exception {
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, SyncMode.CREATE_GROUP);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : ids) {
                assertNull(userManager.getAuthorizable(id));
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncCreateUserAndGroups() throws Exception {
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, new String[] {SyncMode.CREATE_USER,SyncMode.CREATE_GROUP});

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : ids) {
                if (id.equals(userId)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    assertNotNull(a);
                    for (String prop : TestLoginModule.externalUser.getProperties().keySet()) {
                        assertTrue(a.hasProperty(prop));
                    }
                } else {
                    assertNotNull(userManager.getAuthorizable(id));
                }
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncUpdate() throws Exception {
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, SyncMode.UPDATE);

        // create user upfront in order to test update mode
        ExternalUser externalUser = TestLoginModule.externalUser;
        Authorizable user = userManager.createUser(externalUser.getId(), externalUser.getPassword());
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : ids) {
                if (id.equals(userId)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    assertNotNull(a);
                    for (String prop : TestLoginModule.externalUser.getProperties().keySet()) {
                        assertTrue(a.hasProperty(prop));
                    }
                } else {
                    assertNull(userManager.getAuthorizable(id));
                }
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncUpdateAndGroups() throws Exception {
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, new String[] {SyncMode.UPDATE, SyncMode.CREATE_GROUP});

        // create user upfront in order to test update mode
        ExternalUser externalUser = TestLoginModule.externalUser;
        Authorizable user = userManager.createUser(externalUser.getId(), externalUser.getPassword());
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : ids) {
                if (id.equals(userId)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    assertNotNull(a);
                    for (String prop : TestLoginModule.externalUser.getProperties().keySet()) {
                        assertTrue(a.hasProperty(prop));
                    }
                } else {
                    assertNotNull(userManager.getAuthorizable(id));
                }
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testDefaultSync() throws Exception {
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, null);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : ids) {
                if (id.equals(userId)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    assertNotNull(a);
                    for (String prop : TestLoginModule.externalUser.getProperties().keySet()) {
                        assertTrue(a.hasProperty(prop));
                    }
                } else {
                    assertNotNull(userManager.getAuthorizable(id));
                }
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testNoSync() throws Exception {
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, "");

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : ids) {
                assertNull(userManager.getAuthorizable(id));
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry entry = new AppConfigurationEntry(
                        TestLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options);
                return new AppConfigurationEntry[] {entry};
            }
        };
    }



}