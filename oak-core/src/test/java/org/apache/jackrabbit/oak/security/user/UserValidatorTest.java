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
import java.util.List;
import java.util.UUID;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @since OAK 1.0
 */
public class UserValidatorTest extends AbstractSecurityTest implements UserConstants {

    private String userPath;

    @Before
    public void before() throws Exception {
        super.before();
        userPath = getTestUser().getPath();
    }

    @Test
    public void removePassword() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.removeProperty(REP_PASSWORD);
            root.commit();
            fail("removing password should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void removePrincipalName() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.removeProperty(REP_PRINCIPAL_NAME);
            root.commit();
            fail("removing principal name should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void removeAuthorizableId() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.removeProperty(REP_AUTHORIZABLE_ID);
            root.commit();
            fail("removing authorizable id should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void createWithoutPrincipalName() throws Exception {
        try {
            User user = getUserManager(root).createUser("withoutPrincipalName", "pw");
            Tree tree = root.getTree(user.getPath());
            tree.removeProperty(REP_PRINCIPAL_NAME);
            root.commit();

            fail("creating user with invalid jcr:uuid should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void createWithInvalidUUID() throws Exception {
        try {
            User user = getUserManager(root).createUser("withInvalidUUID", "pw");
            Tree tree = root.getTree(user.getPath());
            tree.setProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
            root.commit();

            fail("creating user with invalid jcr:uuid should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void changeUUID() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.setProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
            root.commit();
            fail("changing jcr:uuid should fail if it the uuid valid is invalid");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void changePrincipalName() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.setProperty(REP_PRINCIPAL_NAME, "another");
            root.commit();
            fail("changing the principal name should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void changeAuthorizableId() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.setProperty(REP_AUTHORIZABLE_ID, "modified");
            root.commit();
            fail("changing the authorizable id should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void changePasswordToPlainText() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.setProperty(REP_PASSWORD, "plaintext");
            root.commit();
            fail("storing a plaintext password should fail");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testRemoveAdminUser() throws Exception {
        try {
            String adminId = getConfig().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
            UserManager userMgr = getUserManager(root);
            Authorizable admin = userMgr.getAuthorizable(adminId);
            if (admin == null) {
                admin = userMgr.createUser(adminId, adminId);
                root.commit();
            }

            root.getTree(admin.getPath()).remove();
            root.commit();
            fail("Admin user cannot be removed");
        } catch (CommitFailedException e) {
            // success
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testRemoveAdminUserFolder() throws Exception {
        try {
            String adminId = getConfig().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
            UserManager userMgr = getUserManager(root);
            Authorizable admin = userMgr.getAuthorizable(adminId);
            if (admin == null) {
                admin = userMgr.createUser(adminId, adminId);
                root.commit();
            }

            root.getTree(admin.getPath()).getParent().remove();
            root.commit();
            fail("Admin user cannot be removed");
        } catch (CommitFailedException e) {
            // success
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testDisableAdminUser() throws Exception {
        try {
            String adminId = getConfig().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
            UserManager userMgr = getUserManager(root);
            Authorizable admin = userMgr.getAuthorizable(adminId);
            if (admin == null) {
                admin = userMgr.createUser(adminId, adminId);
                root.commit();
            }

            root.getTree(admin.getPath()).setProperty(REP_DISABLED, "disabled");
            root.commit();
            fail("Admin user cannot be disabled");
        } catch (CommitFailedException e) {
            // success
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testEnforceHierarchy() throws RepositoryException, CommitFailedException {
        List<String> invalid = new ArrayList<String>();
        invalid.add("/");
        invalid.add("/jcr:system");
        String groupRoot = getConfig().getConfigValue(PARAM_GROUP_PATH, DEFAULT_GROUP_PATH);
        invalid.add(groupRoot);
        String userRoot = getConfig().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH);
        invalid.add(Text.getRelativeParent(userRoot, 1));
        invalid.add(userPath);
        invalid.add(userPath + "/folder");

        UserProvider up = new UserProvider(root, getUserConfiguration().getParameters());
        for (String path : invalid) {
            try {
                Tree parent = root.getTree(path);
                if (!parent.exists()) {
                    String[] segments = Text.explode(path, '/', false);
                    parent = root.getTree("/");
                    for (String segment : segments) {
                        Tree next = parent.getChild(segment);
                        if (!next.exists()) {
                            next = parent.addChild(segment);
                            next.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_AUTHORIZABLE_FOLDER, Type.NAME);
                            parent = next;
                        }
                    }
                }
                Tree userTree = parent.addChild("testUser");
                userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
                userTree.setProperty(JcrConstants.JCR_UUID, up.getContentID("testUser"));
                userTree.setProperty(REP_PRINCIPAL_NAME, "testUser");
                root.commit();
                fail("Invalid hierarchy should be detected");

            } catch (CommitFailedException e) {
                // success
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testCreateNestedUser() throws Exception {
        Tree userTree = root.getTree(getTestUser().getPath());
        NodeUtil userNode = new NodeUtil(userTree);
        NodeUtil profile = userNode.addChild("profile", JcrConstants.NT_UNSTRUCTURED);
        NodeUtil nested = profile.addChild("nested", UserConstants.NT_REP_USER);
        nested.setString(UserConstants.REP_PRINCIPAL_NAME, "nested");
        nested.setString(UserConstants.REP_AUTHORIZABLE_ID, "nested");
        nested.setString(JcrConstants.JCR_UUID, UUIDUtils.generateUUID("nested"));
        try {
            root.commit();
            fail("Creating nested users must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(29, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testCreateNestedUser2Steps() throws Exception {
        Tree userTree = root.getTree(getTestUser().getPath());
        NodeUtil userNode = new NodeUtil(userTree);
        NodeUtil profile = userNode.addChild("profile", JcrConstants.NT_UNSTRUCTURED);
        NodeUtil nested = profile.addChild("nested", JcrConstants.NT_UNSTRUCTURED);
        nested.setString(UserConstants.REP_PRINCIPAL_NAME, "nested");
        nested.setString(UserConstants.REP_AUTHORIZABLE_ID, "nested");
        nested.setString(JcrConstants.JCR_UUID, UUIDUtils.generateUUID("nested"));
        root.commit();

        try {
            nested.setName(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_USER);
            root.commit();
            fail("Creating nested users must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(29, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void hiddenNodeAdded() throws CommitFailedException {
        UserValidatorProvider provider = new UserValidatorProvider(getConfig());
        MemoryNodeStore store = new MemoryNodeStore();
        NodeState root = store.getRoot();
        NodeBuilder builder = root.builder();
        NodeBuilder test = builder.child("test");
        NodeBuilder hidden = test.child(":hidden");

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeAdded(
                "test", test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeAdded(
                ":hidden", hidden.getNodeState());
        assertNull(hiddenValidator);
    }

    @Test
    public void hiddenNodeChanged() throws CommitFailedException {
        UserValidatorProvider provider = new UserValidatorProvider(getConfig());
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child(":hidden");
        NodeState root = builder.getNodeState();

        NodeBuilder test = root.builder().child("test");
        NodeBuilder hidden = test.child(":hidden");
        hidden.child("added");

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeChanged(
                "test", root.getChildNode("test"), test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeChanged(
                ":hidden", root.getChildNode("test").getChildNode(":hidden"), hidden.getNodeState());
        assertNull(hiddenValidator);
    }

    @Test
    public void hiddenNodeDeleted() throws CommitFailedException {
        UserValidatorProvider provider = new UserValidatorProvider(getConfig());
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child(":hidden");
        NodeState root = builder.getNodeState();

        builder = root.builder();
        NodeBuilder test = builder.child("test");
        test.child(":hidden").remove();

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeChanged(
                "test", root.getChildNode("test"), test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeDeleted(
                ":hidden", root.getChildNode("test").getChildNode(":hidden"));
        assertNull(hiddenValidator);
    }

    private ConfigurationParameters getConfig() {
        return getUserConfiguration().getParameters();
    }
}