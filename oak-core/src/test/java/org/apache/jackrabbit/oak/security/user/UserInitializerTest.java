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

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.p2.Property2IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.p2.Property2IndexProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.RegistrationEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @since OAK 1.0
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
        Tree oakIndex = root.getTree('/' + IndexConstants.INDEX_DEFINITIONS_NAME);
        assertNotNull(oakIndex);

        Tree id = oakIndex.getChild("authorizableId");
        assertIndexDefinition(id, UserConstants.REP_AUTHORIZABLE_ID, true);

        Tree princName = oakIndex.getChild("principalName");
        assertIndexDefinition(princName, UserConstants.REP_PRINCIPAL_NAME, true);
        String[] declaringNtNames = TreeUtil.getStrings(princName, IndexConstants.DECLARING_NODE_TYPES);
        assertArrayEquals(new String[]{UserConstants.NT_REP_GROUP, UserConstants.NT_REP_USER, UserConstants.NT_REP_AUTHORIZABLE}, declaringNtNames);

        Tree members = oakIndex.getChild("members");
        assertIndexDefinition(members, UserConstants.REP_MEMBERS, false);
    }

    /**
     * @since OAK 1.0 The configuration defines if and how the password of the
     * admin user is being set.
     */
    @Test
    public void testAdminConfiguration() throws Exception {
        Map<String,String> userParams = new HashMap();
        userParams.put(UserConstants.PARAM_ADMIN_ID, "admin");
        userParams.put(UserConstants.PARAM_ADMIN_PW, null);

        ConfigurationParameters params = new ConfigurationParameters(ImmutableMap.of(UserConfiguration.PARAM_USER_OPTIONS, new ConfigurationParameters(userParams)));
        SecurityProvider sp = new SecurityProviderImpl(params);
        final ContentRepository repo = new Oak().with(new InitialContent())
                .with(new Property2IndexHookProvider())
                .with(new Property2IndexProvider())
                .with(new RegistrationEditorProvider())
                .with(sp)
                .createContentRepository();

        ContentSession cs = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<ContentSession>() {
            @Override
            public ContentSession run() throws Exception {
                return repo.login(null, null);
            }
        });
        try {
            Root root = cs.getLatestRoot();
            UserManager umgr = sp.getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);
            Authorizable adminUser = umgr.getAuthorizable("admin");
            assertNotNull(adminUser);

            Tree adminTree = root.getTree(adminUser.getPath());
            assertNotNull(adminTree);
            assertNull(adminTree.getProperty(UserConstants.REP_PASSWORD));
        } finally {
            cs.close();
        }

        // login as admin should fail
        ContentSession adminSession = null;
        try {
            adminSession = repo.login(new SimpleCredentials("admin", new char[0]), null);
            fail();
        } catch (LoginException e) {
            //success
        } finally {
            if (adminSession != null) {
                adminSession.close();
            }
        }
    }

    private static void assertIndexDefinition(Tree tree, String propName, boolean isUnique) {
        assertNotNull(tree);

        assertEquals(isUnique, TreeUtil.getBoolean(tree, IndexConstants.UNIQUE_PROPERTY_NAME));
        assertArrayEquals(propName, new String[]{propName}, TreeUtil.getStrings(tree, IndexConstants.PROPERTY_NAMES));
    }
}
