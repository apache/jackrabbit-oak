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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * UserInitializerTest... TODO
 */
public class UserInitializerTest extends AbstractSecurityTest {

    private UserManager userMgr;
    private ConfigurationParameters config;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        userMgr = getUserManager();
        config = getUserConfiguration().getConfigurationParameters();
    }

    @Test
    public void testBuildInUserExist() throws Exception {
        assertNotNull(userMgr.getAuthorizable(UserUtility.getAdminId(config)));
        assertNotNull(userMgr.getAuthorizable(UserUtility.getAnonymousId(config)));
    }

    @Test
    public void testAdminUser() throws Exception {
        Authorizable a = userMgr.getAuthorizable(UserUtility.getAdminId(config));
        assertFalse(a.isGroup());

        User admin = (User) a;
        assertTrue(admin.isAdmin());
        assertTrue(admin.getPrincipal() instanceof AdminPrincipal);
        assertTrue(admin.getPrincipal() instanceof TreeBasedPrincipal);
        assertEquals(admin.getID(), admin.getPrincipal().getName());
    }

    @Test
    public void testAnonymous() throws Exception {
        Authorizable a = userMgr.getAuthorizable(UserUtility.getAnonymousId(config));
        assertFalse(a.isGroup());

        User anonymous = (User) a;
        assertFalse(anonymous.isAdmin());
        assertFalse(anonymous.getPrincipal() instanceof AdminPrincipal);
        assertTrue(anonymous.getPrincipal() instanceof TreeBasedPrincipal);
        assertEquals(anonymous.getID(), anonymous.getPrincipal().getName());
    }

    @Test
    public void testUserContent() throws Exception {
        Authorizable a = userMgr.getAuthorizable(UserUtility.getAdminId(config));
        assertNotNull(root.getTree(a.getPath()));

        a = userMgr.getAuthorizable(UserUtility.getAnonymousId(config));
        assertNotNull(root.getTree(a.getPath()));
    }

    @Test
    public void testUserIndexDefinitions() throws Exception {
        Tree oakIndex = root.getTree('/' +IndexConstants.INDEX_DEFINITIONS_NAME);
        assertNotNull(oakIndex);

        Tree id = oakIndex.getChild("authorizableId");
        assertIndexDefinition(id, UserConstants.REP_AUTHORIZABLE_ID, true);

        Tree princName = oakIndex.getChild("principalName");
        assertIndexDefinition(princName, UserConstants.REP_PRINCIPAL_NAME, true);

        Tree members = oakIndex.getChild("members");
        assertIndexDefinition(members, UserConstants.REP_MEMBERS, false);
    }

    private static void assertIndexDefinition(Tree tree, String propName, boolean isUnique) {
        assertNotNull(tree);
        NodeUtil node = new NodeUtil(tree);

        assertEquals(isUnique, node.getBoolean(IndexConstants.UNIQUE));
        assertArrayEquals(new String[] {propName}, node.getNames(IndexConstants.PROPERTY_NAMES));
    }
}