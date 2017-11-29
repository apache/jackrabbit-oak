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
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import com.google.common.collect.Iterables;
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
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
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

        userMgr = getUserManager(root);
        config = getUserConfiguration().getParameters();
    }

    @Test
    public void testBuildInUserExist() throws Exception {
        assertNotNull(userMgr.getAuthorizable(UserUtil.getAdminId(config)));
        assertNotNull(userMgr.getAuthorizable(UserUtil.getAnonymousId(config)));
    }

    @Test
    public void testAdminUser() throws Exception {
        Authorizable a = userMgr.getAuthorizable(UserUtil.getAdminId(config));
        assertFalse(a.isGroup());

        User admin = (User) a;
        assertTrue(admin.isAdmin());
        assertTrue(admin.getPrincipal() instanceof AdminPrincipal);
        assertTrue(admin.getPrincipal() instanceof TreeBasedPrincipal);
        assertEquals(admin.getID(), admin.getPrincipal().getName());
    }

    @Test
    public void testAnonymous() throws Exception {
        Authorizable a = userMgr.getAuthorizable(UserUtil.getAnonymousId(config));
        assertFalse(a.isGroup());

        User anonymous = (User) a;
        assertFalse(anonymous.isAdmin());
        assertFalse(anonymous.getPrincipal() instanceof AdminPrincipal);
        assertTrue(anonymous.getPrincipal() instanceof TreeBasedPrincipal);
        assertEquals(anonymous.getID(), anonymous.getPrincipal().getName());
    }

    @Test
    public void testUserContent() throws Exception {
        Authorizable a = userMgr.getAuthorizable(UserUtil.getAdminId(config));
        assertTrue(root.getTree(a.getPath()).exists());

        a = userMgr.getAuthorizable(UserUtil.getAnonymousId(config));
        assertTrue(root.getTree(a.getPath()).exists());
    }

    @Test
    public void testUserIndexDefinitions() throws Exception {
        Tree oakIndex = root.getTree('/' + IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(oakIndex.exists());

        Tree id = oakIndex.getChild("authorizableId");
        assertIndexDefinition(id, UserConstants.REP_AUTHORIZABLE_ID, true);

        Tree princName = oakIndex.getChild("principalName");
        assertIndexDefinition(princName, UserConstants.REP_PRINCIPAL_NAME, true);
        Iterable<String> declaringNtNames = TreeUtil.getStrings(princName, IndexConstants.DECLARING_NODE_TYPES);
        assertArrayEquals(
                new String[]{UserConstants.NT_REP_AUTHORIZABLE},
                Iterables.toArray(declaringNtNames, String.class));

        Tree repMembers = oakIndex.getChild("repMembers");
        assertIndexDefinition(repMembers, UserConstants.REP_MEMBERS, false);
        declaringNtNames = TreeUtil.getStrings(repMembers, IndexConstants.DECLARING_NODE_TYPES);
        assertArrayEquals(
                new String[]{UserConstants.NT_REP_MEMBER_REFERENCES},
                Iterables.toArray(declaringNtNames, String.class));
    }

    /**
     * @since OAK 1.0 The configuration defines if the password of the
     * admin user is being set.
     */
    @Test
    public void testAdminConfiguration() throws Exception {
        Map<String,Object> userParams = new HashMap();
        userParams.put(UserConstants.PARAM_ADMIN_ID, "admin");
        userParams.put(UserConstants.PARAM_OMIT_ADMIN_PW, true);

        ConfigurationParameters params = ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams));
        SecurityProvider sp = new SecurityProviderBuilder().with(params).build();
        final ContentRepository repo = new Oak().with(new InitialContent())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(new TypeEditorProvider())
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
            UserConfiguration uc = sp.getConfiguration(UserConfiguration.class);
            UserManager umgr = uc.getUserManager(root, NamePathMapper.DEFAULT);
            Authorizable adminUser = umgr.getAuthorizable("admin");
            assertNotNull(adminUser);

            Tree adminTree = root.getTree(adminUser.getPath());
            assertTrue(adminTree.exists());
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

    /**
     * @since OAK 1.0 The anonymous user is optional.
     */
    @Test
    public void testAnonymousConfiguration() throws Exception {
        Map<String,Object> userParams = new HashMap();
        userParams.put(UserConstants.PARAM_ANONYMOUS_ID, "");

        ConfigurationParameters params = ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams));
        SecurityProvider sp = new SecurityProviderBuilder().with(params).build();
        final ContentRepository repo = new Oak().with(new InitialContent())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(new TypeEditorProvider())
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
            UserConfiguration uc = sp.getConfiguration(UserConfiguration.class);
            UserManager umgr = uc.getUserManager(root, NamePathMapper.DEFAULT);
            Authorizable anonymous = umgr.getAuthorizable(UserConstants.DEFAULT_ANONYMOUS_ID);
            assertNull(anonymous);
        } finally {
            cs.close();
        }

        // login as admin should fail
        ContentSession anonymousSession = null;
        try {
            anonymousSession = repo.login(new GuestCredentials(), null);
            fail();
        } catch (LoginException e) {
            //success
        } finally {
            if (anonymousSession != null) {
                anonymousSession.close();
            }
        }
    }

    private static void assertIndexDefinition(Tree tree, String propName, boolean isUnique) {
        assertTrue(tree.exists());

        assertEquals(isUnique, TreeUtil.getBoolean(tree, IndexConstants.UNIQUE_PROPERTY_NAME));
        assertArrayEquals(
                propName, new String[]{propName},
                Iterables.toArray(TreeUtil.getStrings(tree, IndexConstants.PROPERTY_NAMES), String.class));
    }
}
