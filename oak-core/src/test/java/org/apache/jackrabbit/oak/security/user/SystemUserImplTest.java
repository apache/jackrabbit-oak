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

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserIdCredentials;
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Implementation specific test wrt system users.
 */
public class SystemUserImplTest extends AbstractSecurityTest {

    private UserManager userMgr;
    private String uid;
    private User user;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        userMgr = getUserManager(root);
        uid = "testUser" + UUID.randomUUID();
    }

    @Override
    public void after() throws Exception {
        try {
            if (user != null) {
                user.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, new AuthorizableActionProvider() {
                    @Nonnull
                    @Override
                    public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
                        AuthorizableAction action = new AccessControlAction();
                        action.init(securityProvider, ConfigurationParameters.of(AccessControlAction.USER_PRIVILEGE_NAMES, new String[]{PrivilegeConstants.JCR_ALL}));
                        return ImmutableList.of(action);
                    }
                }));
    }

    private User createUser(@Nullable String intermediatePath) throws Exception {
        User user = userMgr.createSystemUser(uid, intermediatePath);
        root.commit();
        return user;
    }

    @Test
    public void testCreateSystemUser() throws Exception {
        user = createUser(null);

        assertTrue(user instanceof SystemUserImpl);
    }

    @Test
    public void testSystemUserTree() throws Exception {
        user = createUser(null);
        Tree t = root.getTree(user.getPath());
        assertFalse(t.hasProperty(UserConstants.REP_PASSWORD));
        assertEquals(UserConstants.NT_REP_SYSTEM_USER, TreeUtil.getPrimaryTypeName(t));
    }

    @Test
    public void testGetCredentials() throws Exception {
        user = createUser(null);

        Credentials creds = user.getCredentials();
        assertTrue(creds instanceof UserIdCredentials);

        UserIdCredentials impl = (UserIdCredentials) creds;
        assertEquals(uid, impl.getUserId());
    }

    @Test
    public void testHasNoPassword() throws Exception {
        user = createUser(null);

        Tree userTree = root.getTree(user.getPath());
        assertFalse(userTree.hasProperty(UserConstants.REP_PASSWORD));
    }


    /**
     * @since OAK 1.0 In contrast to Jackrabbit core the intermediate path may
     * not be an absolute path in OAK.
     */
    @Test
    public void testCreateUserWithAbsolutePath() throws Exception {
        try {
            user = createUser("/any/path/to/the/new/user");
            fail("ConstraintViolationException expected");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testCreateUserWithAbsolutePath2() throws Exception {
        try {
            user = createUser(UserConstants.DEFAULT_USER_PATH + "/any/path/to/the/new/user");
            fail("ConstraintViolationException expected");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testCreateUserWithAbsolutePath3() throws Exception {
        String userRoot = UserConstants.DEFAULT_USER_PATH + '/' + UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH;
        String path = userRoot + "/any/path/to/the/new/user";

        user = createUser(path);
        assertTrue(user.getPath().startsWith(path));
    }

    @Test
    public void testCreateUserWithRelativePath() throws Exception {
        try {
            user = createUser("any/path");
            fail("ConstraintViolationException expected");
        }  catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testCreateUserWithRelativePath2() throws Exception {
        user = createUser(UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH + "/any/path");

        assertNotNull(user.getID());
        assertTrue(user.getPath().contains("any/path"));
    }

    @Test
    public void testCreateSystemUserWithOtherPath() throws Exception {
        String path = null;
        try {
            Tree t = root.getTree(UserConstants.DEFAULT_USER_PATH);
            NodeUtil systemUserTree = new NodeUtil(t).addChild("systemUser", UserConstants.NT_REP_SYSTEM_USER);
            systemUserTree.setString(UserConstants.REP_PRINCIPAL_NAME, "systemUser");
            systemUserTree.setString(UserConstants.REP_AUTHORIZABLE_ID, "systemUser");
            path = systemUserTree.getTree().getPath();

            root.commit();
            fail();
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
        } finally {
            root.refresh();
            if (path != null) {
                Tree t = root.getTree(path);
                if (t.exists()) {
                    t.remove();
                    root.commit();
                }
            }
        }
    }

    @Test
    public void testLoginAsSystemUser() throws Exception {
        user = createUser(null);
        try {
            login(new SimpleCredentials(uid, new char[0])).close();
            fail();
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testLoginAsSystemUser2() throws Exception {
        user = createUser(null);
        try {
            login(user.getCredentials()).close();
            fail();
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testImpersonateSystemUser() throws Exception {
        user = createUser(null);
        ContentSession cs = login(new ImpersonationCredentials(new SimpleCredentials(uid, new char[0]), adminSession.getAuthInfo()));
        cs.close();
    }


    @Test
    public void testImpersonateDisabledSystemUser() throws Exception {
        user = createUser(null);
        user.disable("disabled");
        root.commit();
        try {
            ContentSession cs = login(new ImpersonationCredentials(new SimpleCredentials(uid, new char[0]), adminSession.getAuthInfo()));
            cs.close();
            fail();
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testGetPrincipal() throws Exception {
        user = createUser(null);
        assertTrue(user.getPrincipal() instanceof SystemUserPrincipal);
    }

    @Test
    public void testAddToGroup() throws Exception {
        user = createUser(null);


        Group g = null;
        try {
            g = userMgr.createGroup("testGroup");
            g.addMember(user);
            root.commit();

            assertTrue(g.isMember(user));
            assertTrue(g.isDeclaredMember(user));

            boolean isMemberOfG = false;
            Iterator<Group> groups = user.declaredMemberOf();
            while (groups.hasNext() && !isMemberOfG) {
                if (g.getID().equals(groups.next().getID())) {
                    isMemberOfG = true;
                }
            }
            assertTrue(isMemberOfG);
        } finally {
            if (g != null) {
                g.remove();
                root.commit();
            }
        }
    }

    /**
     * Test asserting that {@link AuthorizableAction#onCreate(User, String, Root, NamePathMapper)}
     * is omitted upon calling {@link UserManager#createSystemUser(String, String)}.
     */
    @Test
    public void testOnCreateOmitted() throws Exception {
        user = createUser(null);

        Tree t = root.getTree(user.getPath());
        assertFalse(t.hasChild(AccessControlConstants.REP_POLICY));
    }
}