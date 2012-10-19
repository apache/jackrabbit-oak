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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexHook;
import org.apache.jackrabbit.oak.plugins.nodetype.InitialContent;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * UserProviderImplTest...
 *
 * TODO: create tests with custom config that persists changes (currently fails since config used in UserValidator is different)
 * TODO: add tests for setProtectedProperty (might still be refactored...)
 * TODO: add tests for findAuthorizables once implementation is ready
 */
public class UserProviderTest extends AbstractSecurityTest {

    private ContentSession contentSession;
    private Root root;

    private ConfigurationParameters defaultConfig;
    private String defaultUserPath;
    private String defaultGroupPath;
    
    private Map<String, Object> customOptions;
    private String customUserPath = "/home/users";
    private String customGroupPath = "/home/groups";

    private List<String> cleanupPaths = new ArrayList<String>();

    @Before
    public void setUp() throws Exception {
        contentSession = login(getAdminCredentials());
        root = contentSession.getLatestRoot();

        defaultConfig = new ConfigurationParameters();
        defaultUserPath = defaultConfig.getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH);
        defaultGroupPath = defaultConfig.getConfigValue(UserConstants.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH);

        customOptions = new HashMap<String, Object>();
        customOptions.put(UserConstants.PARAM_GROUP_PATH, customGroupPath);
        customOptions.put(UserConstants.PARAM_USER_PATH, customUserPath);

        cleanupPaths.add(defaultUserPath);
        cleanupPaths.add(defaultGroupPath);
        cleanupPaths.add(customUserPath);
        cleanupPaths.add(customGroupPath);
    }

    @After
    public void tearDown() throws Exception {
        for (String path : cleanupPaths) {
            Tree t = root.getTree(path);
            if (t != null) {
                t.remove();
            }
        }
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
            .with(new InitialContent())
            .with(new PropertyIndexHook())
            .createContentRepository();
    }

    private UserProvider createUserProvider() {
        return new UserProvider(root, defaultConfig);
    }

    private UserProvider createUserProvider(int defaultDepth) {
        Map<String, Object> options = new HashMap<String, Object>(customOptions);
        options.put(UserConstants.PARAM_DEFAULT_DEPTH, defaultDepth);
        return new UserProvider(root, new ConfigurationParameters(options));
    }

    @Test
    public void testCreateUser() throws Exception {
        UserProvider up = createUserProvider();

        // create test user
        Tree userTree = up.createUser("user1", null);

        assertNotNull(userTree);
        assertTrue(Text.isDescendant(defaultUserPath, userTree.getPath()));
        int level = defaultConfig.getConfigValue(UserConstants.PARAM_DEFAULT_DEPTH, UserConstants.DEFAULT_DEPTH) + 1;
        assertEquals(defaultUserPath, Text.getRelativeParent(userTree.getPath(), level));
        
        // make sure all users are created in a structure with default depth
        userTree = up.createUser("b", null);
        assertEquals(defaultUserPath + "/b/bb/b", userTree.getPath());

        Map<String, String> m = new HashMap<String,String>();
        m.put("bb",     "/b/bb/bb");
        m.put("bbb",    "/b/bb/bbb");
        m.put("bbbb",   "/b/bb/bbbb");
        m.put("bh",     "/b/bh/bh");
        m.put("bHbh",   "/b/bH/bHbh");
        m.put("b_Hb",   "/b/b_/b_Hb");
        m.put("basim", "/b/ba/basim");

        for (String uid : m.keySet()) {
            userTree = up.createUser(uid, null);
            assertEquals(defaultUserPath + m.get(uid), userTree.getPath());
        }
    }

    @Test
    public void testCreateUserWithPath() throws Exception {
        UserProvider up = createUserProvider(1);

        // create test user
        Tree userTree = up.createUser("nadine", "a/b/c");
        assertNotNull(userTree);
        assertTrue(Text.isDescendant(customUserPath, userTree.getPath()));
        String userPath = customUserPath + "/a/b/c/nadine";
        assertEquals(userPath, userTree.getPath());
    }

    @Test
    public void testCreateGroup() throws RepositoryException {
        UserProvider up = createUserProvider();

        Tree groupTree = up.createGroup("group1", null);

        assertNotNull(groupTree);
        assertTrue(Text.isDescendant(defaultGroupPath, groupTree.getPath()));

        int level = defaultConfig.getConfigValue(UserConstants.PARAM_DEFAULT_DEPTH, UserConstants.DEFAULT_DEPTH) + 1;
        assertEquals(defaultGroupPath, Text.getRelativeParent(groupTree.getPath(), level));
    }

    @Test
    public void testCreateGroupWithPath() throws Exception {
        UserProvider up = createUserProvider(4);

        // create test user
        Tree group = up.createGroup("authors", "a/b/c");
        assertNotNull(group);
        assertTrue(Text.isDescendant(customGroupPath, group.getPath()));
        String groupPath = customGroupPath + "/a/b/c/authors";
        assertEquals(groupPath, group.getPath());
    }

    @Test
    public void testCreateWithCustomDepth() throws Exception {
        UserProvider userProvider = createUserProvider(3);

        Tree userTree = userProvider.createUser("b", null);
        assertEquals(customUserPath + "/b/bb/bbb/b", userTree.getPath());

        Map<String, String> m = new HashMap<String,String>();
        m.put("bb",     "/b/bb/bbb/bb");
        m.put("bbb",    "/b/bb/bbb/bbb");
        m.put("bbbb",   "/b/bb/bbb/bbbb");
        m.put("bL",     "/b/bL/bLL/bL");
        m.put("bLbh",   "/b/bL/bLb/bLbh");
        m.put("b_Lb",   "/b/b_/b_L/b_Lb");
        m.put("basiL",  "/b/ba/bas/basiL");

        for (String uid : m.keySet()) {
            userTree = userProvider.createUser(uid, null);
            assertEquals(customUserPath + m.get(uid), userTree.getPath());
        }
    }

    @Test
    public void testCreateWithCollision() throws Exception {
        UserProvider userProvider = createUserProvider();

        Tree userTree = userProvider.createUser("AmaLia", null);

        Map<String, String> colliding = new HashMap<String, String>();
        colliding.put("AmaLia", null);
        colliding.put("AmaLia", "s/ome/path");
        colliding.put("amalia", null);
        colliding.put("Amalia", "a/b/c");

        for (String uid : colliding.keySet()) {
            try {
                Tree c = userProvider.createUser(uid, colliding.get(uid));
                root.commit();
                fail("userID collision must be detected");
            } catch (CommitFailedException e) {
                // success
            }
        }

        for (String uid : colliding.keySet()) {
            try {
                Tree c = userProvider.createGroup(uid, colliding.get(uid));
                root.commit();
                fail("userID collision must be detected");
            } catch (CommitFailedException e) {
                // success
            }
        }
    }

    @Test
    public void testIllegalChars() throws Exception {
        UserProvider userProvider = createUserProvider();

        Map<String, String> m = new HashMap<String, String>();
        m.put("z[x]", "/z/" + Text.escapeIllegalJcrChars("z[") + '/' + Text.escapeIllegalJcrChars("z[x]"));
        m.put("z*x", "/z/" + Text.escapeIllegalJcrChars("z*") + '/' + Text.escapeIllegalJcrChars("z*x"));
        m.put("z/x", "/z/" + Text.escapeIllegalJcrChars("z/") + '/' + Text.escapeIllegalJcrChars("z/x"));
        m.put("%\r|", '/' +Text.escapeIllegalJcrChars("%")+ '/' + Text.escapeIllegalJcrChars("%\r") + '/' + Text.escapeIllegalJcrChars("%\r|"));

        for (String uid : m.keySet()) {
            Tree user = userProvider.createUser(uid, null);
            root.commit();

            assertEquals(defaultUserPath + m.get(uid), user.getPath());
            assertEquals(uid, userProvider.getAuthorizableId(user));

            Tree ath = userProvider.getAuthorizable(uid);
            assertNotNull("Tree with id " + uid + " must exist.", ath);
        }
    }

    @Test
    public void testGetAuthorizable() throws Exception {
        UserProvider up = createUserProvider();

        String userID = "hannah";
        String groupID = "cLevel";

        Tree user = up.createUser(userID, null);
        Tree group = up.createGroup(groupID, null);
        root.commit();

        Tree a = up.getAuthorizable(userID);
        assertNotNull(a);
        assertEquals(user.getPath(), a.getPath());

        a = up.getAuthorizable(groupID);
        assertNotNull(a);
        assertEquals(group.getPath(), a.getPath());
    }

    @Test
    public void testGetAuthorizableByPath() throws Exception {
        UserProvider up = createUserProvider();

        Tree user = up.createUser("shams", null);
        Tree a = up.getAuthorizableByPath(user.getPath());
        assertNotNull(a);
        assertEquals(user, a);

        Tree group = up.createGroup("devs", null);
        a = up.getAuthorizableByPath(group.getPath());
        assertNotNull(a);
        assertEquals(group, a);
    }

    @Test
    public void testGetAuthorizableId() throws Exception {
        UserProvider up = createUserProvider();

        String userID = "Amanda";
        Tree user = up.createUser(userID, null);
        assertEquals(userID, up.getAuthorizableId(user));

        String groupID = "visitors";
        Tree group = up.createGroup(groupID, null);
        assertEquals(groupID, up.getAuthorizableId(group));
    }

    @Test
    public void testRemoveParentTree() throws Exception {
        UserProvider up = createUserProvider();
        Tree u1 = up.createUser("b", "b");
        Tree u2 = up.createUser("bb", "bb");

        Tree folder = root.getTree(Text.getRelativeParent(u1.getPath(), 2));
        folder.remove();
        if (up.getAuthorizable("b") != null) {
            fail("Removing the top authorizable folder must remove all users contained.");
            u1.remove();
        }
        if (up.getAuthorizable("bb") != null) {
            fail("Removing the top authorizable folder must remove all users contained.");
            u2.remove();
        }
    }
}
