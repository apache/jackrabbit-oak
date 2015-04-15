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

import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * ExternalLoginModuleTest...
 */
public class ExternalLoginModuleTest extends ExternalLoginModuleTestBase {

    protected final HashMap<String, Object> options = new HashMap<String, Object>();

    private final String userId = "testUser";

    private final static String TEST_CONSTANT_PROPERTY_NAME = "profile/constantProperty";

    private final static String TEST_CONSTANT_PROPERTY_VALUE = "constant-value";

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

    @Test
    public void testLoginFailed() throws Exception {
        UserManager userManager = getUserManager(root);
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
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        try {
            assertNull(userManager.getAuthorizable(userId));

            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();

            Authorizable a = userManager.getAuthorizable(userId);
            assertNotNull(a);
            ExternalUser user = idp.getUser(userId);
            for (String prop : user.getProperties().keySet()) {
                assertTrue(a.hasProperty(prop));
            }
            assertEquals(TEST_CONSTANT_PROPERTY_VALUE, a.getProperty(TEST_CONSTANT_PROPERTY_NAME)[0].getString());
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncCreateUserCaseInsensitive() throws Exception {
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        try {
            assertNull(userManager.getAuthorizable(userId));

            cs = login(new SimpleCredentials(userId.toUpperCase(), new char[0]));

            root.refresh();

            Authorizable a = userManager.getAuthorizable(userId);
            assertNotNull(a);
            ExternalUser user = idp.getUser(userId);
            for (String prop : user.getProperties().keySet()) {
                assertTrue(a.hasProperty(prop));
            }
            assertEquals(TEST_CONSTANT_PROPERTY_VALUE, a.getProperty(TEST_CONSTANT_PROPERTY_NAME)[0].getString());
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncCreateGroup() throws Exception {
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : new String[]{"a", "b", "c"}) {
                assertNotNull(userManager.getAuthorizable(id));
            }
            for (String id : new String[]{"aa", "aaa"}) {
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
    public void testSyncCreateGroupNesting() throws Exception {
        syncConfig.user().setMembershipNestingDepth(2);
        UserManager userManager = getUserManager(root);
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();
            for (String id : new String[]{"a", "b", "c", "aa", "aaa"}) {
                assertNotNull(userManager.getAuthorizable(id));
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
        // create user upfront in order to test update mode
        UserManager userManager = getUserManager(root);
        ExternalUser externalUser = idp.getUser(userId);
        Authorizable user = userManager.createUser(externalUser.getId(), null);
        user.setProperty("rep:externalId", new ValueFactoryImpl(root, NamePathMapper.DEFAULT).createValue(externalUser.getExternalId().getString()));
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(userId, new char[0]));

            root.refresh();

            Authorizable a = userManager.getAuthorizable(userId);
            assertNotNull(a);
            for (String prop : externalUser.getProperties().keySet()) {
                assertTrue(a.hasProperty(prop));
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

}