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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @since OAK 1.0
 */
public class UserProviderTest {

    private Root root;

    private ConfigurationParameters defaultConfig;
    private String defaultUserPath;
    private String defaultGroupPath;
    
    private Map<String, Object> customOptions;
    private String customUserPath = "/home/users";
    private String customGroupPath = "/home/groups";

    @Before
    public void setUp() throws Exception {
        root = new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(new PropertyIndexEditorProvider())
                .createRoot();

        defaultConfig = ConfigurationParameters.EMPTY;
        defaultUserPath = defaultConfig.getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH);
        defaultGroupPath = defaultConfig.getConfigValue(UserConstants.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH);

        customOptions = new HashMap<String, Object>();
        customOptions.put(UserConstants.PARAM_GROUP_PATH, customGroupPath);
        customOptions.put(UserConstants.PARAM_USER_PATH, customUserPath);
    }

    @After
    public void tearDown() {
        root.refresh();
        root = null;
    }

    private UserProvider createUserProvider() {
        return new UserProvider(root, defaultConfig);
    }

    private UserProvider createUserProvider(int defaultDepth) {
        Map<String, Object> options = new HashMap<String, Object>(customOptions);
        options.put(UserConstants.PARAM_DEFAULT_DEPTH, defaultDepth);
        return new UserProvider(root, ConfigurationParameters.of(options));
    }

    private UserProvider createUserProviderRFC7612() {
        Map<String, Object> options = new HashMap<String, Object>(customOptions);
        options.put(UserConstants.PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE, true);
        return new UserProvider(root, ConfigurationParameters.of(options));
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
    public void testCreateUserRFC7613Disabled() throws Exception {
        String userHalfWidth = "Amalia";
        String userFullWidth = "\uff21\uff4d\uff41\uff4c\uff49\uff41";

        UserProvider userProvider = createUserProvider();

        Tree userTreeHalfWidth = userProvider.createUser(userHalfWidth, null);
        Tree userTreeFullWidth = userProvider.createUser(userFullWidth, null);

        root.commit();

        assertEquals(userHalfWidth, UserUtil.getAuthorizableId(userTreeHalfWidth));
        assertEquals(userFullWidth, UserUtil.getAuthorizableId(userTreeFullWidth));
    }

    @Test
    public void testCreateUserRFC7613Enabled() throws Exception {
        String userHalfWidth = "Amalia";
        String userFullWidth = "\uff21\uff4d\uff41\uff4c\uff49\uff41";

        UserProvider userProvider = createUserProviderRFC7612();

        userProvider.createUser(userHalfWidth, null);

        try {
            userProvider.createUser(userFullWidth, null);
            root.commit();
            fail("userID collision must be detected");
        } catch (CommitFailedException e) {
            // success
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
            assertEquals(uid, UserUtil.getAuthorizableId(user));

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
        assertEquals(user.getPath(), a.getPath());

        Tree group = up.createGroup("devs", null);
        a = up.getAuthorizableByPath(group.getPath());
        assertNotNull(a);
        assertEquals(group.getPath(), a.getPath());
    }

    @Test
    public void testGetAuthorizableId() throws Exception {
        UserProvider up = createUserProvider();

        String userID = "Amanda";
        Tree user = up.createUser(userID, null);
        assertEquals(userID, UserUtil.getAuthorizableId(user));

        String groupID = "visitors";
        Tree group = up.createGroup(groupID, null);
        assertEquals(groupID, UserUtil.getAuthorizableId(group));
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

    @Test
    public void testCollisions() throws Exception {
        ConfigurationParameters config = ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, new AuthorizableNodeName() {
            @Nonnull
            @Override
            public String generateNodeName(@Nonnull String authorizableId) {
                return "aaa";
            }
        });
        UserProvider up = new UserProvider(root, config);

        try {
            Tree u1 = up.createUser("a", null);
            assertEquals("aaa", u1.getName());
            Tree u2 = up.createUser("b", null);
            assertEquals("aaa1", u2.getName());
            Tree u3 = up.createUser("c", null);
            assertEquals("aaa2", u3.getName());
            Tree u4 = up.createUser("d", null);
            assertEquals("aaa3", u4.getName());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testAutoCreatedItemsUponUserCreation() throws Exception {
        UserProvider up = createUserProvider();
        assertAutoCreatedItems(up.createUser("c", null), UserConstants.NT_REP_USER, root);
    }

    @Test
    public void testAutoCreatedItemsUponSystemUserCreation() throws Exception {
        UserProvider up = createUserProvider();
        assertAutoCreatedItems(up.createSystemUser("s", null), UserConstants.NT_REP_SYSTEM_USER, root);
    }

    @Test
    public void testAutoCreatedItemsUponGroupCreation() throws Exception {
        UserProvider up = createUserProvider();
        assertAutoCreatedItems(up.createGroup("g", null), UserConstants.NT_REP_GROUP, root);
    }

    private static void assertAutoCreatedItems(@Nonnull Tree authorizableTree, @Nonnull String ntName, @Nonnull Root root) throws Exception {
        NodeType repUser = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT).getNodeType(ntName);
        for (NodeDefinition cnd : repUser.getChildNodeDefinitions()) {
            if (cnd.isAutoCreated()) {
                assertTrue(authorizableTree.hasChild(cnd.getName()));
            }
        }

        for (PropertyDefinition pd : repUser.getPropertyDefinitions()) {
            if (pd.isAutoCreated()) {
                assertTrue(authorizableTree.hasProperty(pd.getName()));
            }
        }
    }
}
