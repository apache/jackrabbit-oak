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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @After
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @NotNull
    private UserValidatorProvider createValidatorProvider() {
        return new UserValidatorProvider(getConfig(), getRootProvider(), getTreeProvider());
    }

    @NotNull
    private UserValidator createUserValidator(@NotNull Tree before, @NotNull Tree after) {
        UserValidatorProvider uvp = createValidatorProvider();
        // force creation of membership provider
        uvp.getRootValidator(getTreeProvider().asNodeState(before), getTreeProvider().asNodeState(after), new CommitInfo("sid", null));
        return new UserValidator(before, after, uvp);
    }

    private void assertRemoveProperty(@NotNull String name, int expectedCode) throws CommitFailedException {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.removeProperty(name);
            root.commit();
        } catch (CommitFailedException e) {
            assertEquals(expectedCode, e.getCode());
            assertTrue(e.isConstraintViolation());
            throw e;
        }
    }

    private void assertChangeProperty(@NotNull String name, @NotNull String value, int expectedCode) throws CommitFailedException {
        try {
            Tree userTree = root.getTree(userPath);
            userTree.setProperty(name, value);
            root.commit();
        } catch (CommitFailedException e) {
            assertEquals(expectedCode, e.getCode());
            assertTrue(e.isConstraintViolation());
            throw e;
        }
    }

    @NotNull
    private ConfigurationParameters getConfig() {
        return getUserConfiguration().getParameters();
    }

    @Test(expected = CommitFailedException.class)
    public void removePassword() throws Exception {
        assertRemoveProperty(REP_PASSWORD, 25);
    }

    @Test(expected = CommitFailedException.class)
    public void removePrincipalName() throws Exception {
        assertRemoveProperty(REP_PRINCIPAL_NAME, 22);
    }

    @Test(expected = CommitFailedException.class)
    public void removePrincipalName2() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            UserValidator validator = createUserValidator(userTree, userTree);
            validator.propertyDeleted(userTree.getProperty(REP_PRINCIPAL_NAME));
        } catch (CommitFailedException e) {
            assertEquals(25, e.getCode());
            assertTrue(e.isConstraintViolation());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void removeAuthorizableId() throws Exception {
        assertRemoveProperty(REP_AUTHORIZABLE_ID, 25);
    }

    @Test(expected = CommitFailedException.class)
    public void createWithoutPrincipalName() throws Exception {
        User user = getUserManager(root).createUser("withoutPrincipalName", "pw");
        Tree tree = root.getTree(user.getPath());
        tree.removeProperty(REP_PRINCIPAL_NAME);
        root.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void createWithoutPrincipalName2() throws Exception {
        Tree userTree = root.getTree(userPath);
        userTree.removeProperty(REP_PRINCIPAL_NAME);
        NodeState userState = getTreeProvider().asNodeState(userTree);

        try {
            Tree tree = root.getTree(userPath).getParent();
            createUserValidator(tree, tree).childNodeAdded(userTree.getName(), userState);
        } catch (CommitFailedException e) {
            assertEquals(26, e.getCode());
            throw e;
        }
    }

    @Test
    public void createWithoutAuthorizableId() throws Exception {
        User user = getUserManager(root).createUser("withoutId", "pw");
        Tree tree = root.getTree(user.getPath());
        tree.removeProperty(REP_AUTHORIZABLE_ID);
        root.commit();

        assertNotNull(getUserManager(root).getAuthorizable("withoutId"));
    }

    @Test(expected = CommitFailedException.class)
    public void createWithInvalidUUID() throws Exception {
        User user = getUserManager(root).createUser("withInvalidUUID", "pw");
        Tree tree = root.getTree(user.getPath());
        tree.setProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString());
        root.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void createSystemUserWithPw() throws Exception {
        try {
            User user = getUserManager(root).createSystemUser("withPw", null);
            Tree tree = root.getTree(user.getPath());
            tree.setProperty(REP_PASSWORD, "pw", Type.STRING);
            root.commit();
        } catch (CommitFailedException e) {
            assertEquals(32, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void createSystemUserWithPwNode() throws Exception {
        try {
            User user = getUserManager(root).createSystemUser("withPwNode", null);
            Tree tree = root.getTree(user.getPath());
            TreeUtil.addChild(tree, REP_PWD, NT_REP_PASSWORD);
            root.commit();
        } catch (CommitFailedException e) {
            assertEquals(33, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void changeUUID() throws Exception {
        assertChangeProperty(JcrConstants.JCR_UUID, UUID.randomUUID().toString(), 23);
    }

    @Test
    public void changeUUIDValid() throws Exception {
        Tree userTree = root.getTree(userPath);
        UserValidator validator = createUserValidator(userTree, userTree);
        validator.propertyChanged(PropertyStates.createProperty(JCR_UUID, "invalidBefore"), userTree.getProperty(JCR_UUID));
    }

    @Test(expected = CommitFailedException.class)
    public void changePrincipalName() throws Exception {
        assertChangeProperty(REP_PRINCIPAL_NAME, "another", 22);
    }

    @Test(expected = CommitFailedException.class)
    public void changeAuthorizableId() throws Exception {
        assertChangeProperty(REP_AUTHORIZABLE_ID, "modified", 22);
    }

    @Test(expected = CommitFailedException.class)
    public void changePasswordToPlainText() throws Exception {
        assertChangeProperty(REP_PASSWORD, "plaintext", 24);
    }

    @Test(expected = CommitFailedException.class)
    public void changePasswordToPlainText2() throws Exception {
        Tree beforeTree = when(mock(Tree.class).getProperty(JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)).getMock();
        Tree afterTree = root.getTree(userPath);

        UserValidator uv = new UserValidator(beforeTree, afterTree, new UserValidatorProvider(ConfigurationParameters.EMPTY, getRootProvider(), getTreeProvider()));
        PropertyState plainTextAfter = PropertyStates.createProperty(REP_PASSWORD, "pw");
        uv.propertyChanged(afterTree.getProperty(REP_PASSWORD), plainTextAfter);
    }

    @Test(expected = CommitFailedException.class)
    public void changePrimaryType() throws Exception {
        try {
            Tree userTree = root.getTree(userPath);
            UserValidator validator = createUserValidator(userTree, userTree);
            validator.propertyChanged(userTree.getProperty(JCR_PRIMARYTYPE), PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_REP_GROUP));
        } catch (CommitFailedException e) {
            assertEquals(28, e.getCode());
            throw e;
        }
    }

    @Test
    public void changePrimaryTypeValid() throws Exception {
        Tree userTree = root.getTree(userPath);
        UserValidator validator = createUserValidator(userTree, userTree);
        validator.propertyChanged(PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_REP_GROUP), userTree.getProperty(JCR_PRIMARYTYPE));
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveAdminUser() throws Exception {
        String adminId = getConfig().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
        UserManager userMgr = getUserManager(root);
        Authorizable admin = userMgr.getAuthorizable(adminId);
        if (admin == null) {
            admin = userMgr.createUser(adminId, adminId);
            root.commit();
        }

        root.getTree(admin.getPath()).remove();
        root.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveAdminUserFolder() throws Exception {
        String adminId = getConfig().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
        UserManager userMgr = getUserManager(root);
        Authorizable admin = userMgr.getAuthorizable(adminId);
        if (admin == null) {
            admin = userMgr.createUser(adminId, adminId);
            root.commit();
        }

        try {
            root.getTree(admin.getPath()).getParent().remove();
            root.commit();
        } catch (CommitFailedException e) {
            assertEquals(27, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
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
        } catch (CommitFailedException e) {
            assertEquals(20, e.getCode());
            throw e;
        }
    }

    @Test
    public void testDisableAdminUserNonExistingTree() throws Exception {
        String adminId = getConfig().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
        Authorizable admin = getUserManager(root).getAuthorizable(adminId);

        Tree userTree = root.getTree(checkNotNull(admin).getPath());
        UserValidator validator = createUserValidator(userTree, userTree);
        userTree.remove();

        validator.propertyAdded(PropertyStates.createProperty(REP_DISABLED, "disabled"));
    }

    @Test
    public void testEnforceHierarchy() {
        List<String> invalid = new ArrayList<>();
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
                            next.setProperty(JCR_PRIMARYTYPE, NT_REP_AUTHORIZABLE_FOLDER, Type.NAME);
                            parent = next;
                        }
                    }
                }
                Tree userTree = parent.addChild("testUser");
                userTree.setProperty(JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
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

    @Test(expected = CommitFailedException.class)
    public void testCreateNestedUser() throws Exception {
        Tree userTree = root.getTree(getTestUser().getPath());
        Tree profile = TreeUtil.addChild(userTree, "profile", JcrConstants.NT_UNSTRUCTURED);
        Tree nested = TreeUtil.addChild(profile, "nested", UserConstants.NT_REP_USER);
        nested.setProperty(UserConstants.REP_PRINCIPAL_NAME, "nested");
        nested.setProperty(UserConstants.REP_AUTHORIZABLE_ID, "nested");
        nested.setProperty(JcrConstants.JCR_UUID, UUIDUtils.generateUUID("nested"));
        try {
            root.commit();
            fail("Creating nested users must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(29, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testCreateNestedUser2Steps() throws Exception {
        Tree userTree = root.getTree(getTestUser().getPath());
        Tree profile = TreeUtil.addChild(userTree, "profile", JcrConstants.NT_UNSTRUCTURED);
        Tree nested = TreeUtil.addChild(profile, "nested", JcrConstants.NT_UNSTRUCTURED);
        nested.setProperty(UserConstants.REP_PRINCIPAL_NAME, "nested");
        nested.setProperty(UserConstants.REP_AUTHORIZABLE_ID, "nested");
        nested.setProperty(JcrConstants.JCR_UUID, UUIDUtils.generateUUID("nested"));
        root.commit();

        try {
            nested.setProperty(JCR_PRIMARYTYPE, UserConstants.NT_REP_USER, Type.NAME);
            root.commit();
            fail("Creating nested users must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(29, e.getCode());
            throw e;
        }
    }

    @Test
    public void hiddenNodeAdded() throws CommitFailedException {
        UserValidatorProvider provider = createValidatorProvider();
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
        UserValidatorProvider provider = createValidatorProvider();
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
        UserValidatorProvider provider = createValidatorProvider();
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
}