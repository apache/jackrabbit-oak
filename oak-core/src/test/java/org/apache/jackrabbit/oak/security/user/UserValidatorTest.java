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
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * UserValidatorTest
 */
public class UserValidatorTest extends AbstractSecurityTest {

    private Root root;
    private UserManagerImpl userMgr;
    private User user;

    @Before
    public void before() throws Exception {
        super.before();

        root = adminSession.getLatestRoot();
        userMgr = new UserManagerImpl(root, NamePathMapper.DEFAULT, getSecurityProvider());
        user = userMgr.createUser("test", "pw");
        root.commit();
    }

    @After
    public void after() throws Exception {
        try {
            Authorizable a = userMgr.getAuthorizable("test");
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void removePassword() throws Exception {
        try {
            Tree userTree = root.getTree(user.getPath());
            userTree.removeProperty(UserConstants.REP_PASSWORD);
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
            Tree userTree = root.getTree(user.getPath());
            userTree.removeProperty(UserConstants.REP_PRINCIPAL_NAME);
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
            Tree userTree = root.getTree(user.getPath());
            userTree.removeProperty(UserConstants.REP_AUTHORIZABLE_ID);
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
            User user = userMgr.createUser("withoutPrincipalName", "pw");
            // FIXME: use user.getPath instead (blocked by OAK-343)
            Tree tree = root.getTree("/rep:security/rep:authorizables/rep:users/t/te/test");
            tree.removeProperty(UserConstants.REP_PRINCIPAL_NAME);
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
            User user = userMgr.createUser("withInvalidUUID", "pw");
            // FIXME: use user.getPath instead (blocked by OAK-343)
            Tree tree = root.getTree("/rep:security/rep:authorizables/rep:users/t/te/test");
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
            Tree userTree = root.getTree(user.getPath());
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
            Tree userTree = root.getTree(user.getPath());
            userTree.setProperty(UserConstants.REP_PRINCIPAL_NAME, "another");
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
            Tree userTree = root.getTree(user.getPath());
            userTree.setProperty(UserConstants.REP_AUTHORIZABLE_ID, "modified");
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
            Tree userTree = root.getTree(user.getPath());
            userTree.setProperty(UserConstants.REP_PASSWORD, "plaintext");
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
            String adminId = userMgr.getConfig().getConfigValue(UserConstants.PARAM_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID);
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
    public void testDisableAdminUser() throws Exception {
        try {
            String adminId = userMgr.getConfig().getConfigValue(UserConstants.PARAM_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID);
            Authorizable admin = userMgr.getAuthorizable(adminId);
            if (admin == null) {
                admin = userMgr.createUser(adminId, adminId);
                root.commit();
            }

            root.getTree(admin.getPath()).setProperty(UserConstants.REP_DISABLED, "disabled");
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
        String groupPath = userMgr.getConfig().getConfigValue(UserConstants.PARAM_GROUP_PATH, UserConstants.DEFAULT_GROUP_PATH);
        invalid.add(groupPath);
        String userPath = userMgr.getConfig().getConfigValue(UserConstants.PARAM_USER_PATH, UserConstants.DEFAULT_USER_PATH);
        invalid.add(Text.getRelativeParent(userPath, 1));
        invalid.add(user.getPath());
        invalid.add(user.getPath() + "/folder");

        for (String path : invalid) {
            try {
                Tree parent = root.getTree(path);
                if (parent == null) {
                    String[] segments = Text.explode(path, '/', false);
                    parent = root.getTree("/");
                    for (String segment : segments) {
                        Tree next = parent.getChild(segment);
                        if (next == null) {
                            next = parent.addChild(segment);
                            next.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
                            parent = next;
                        }
                    }
                }
                Tree userTree = parent.addChild("testUser");
                userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_USER);
                userTree.setProperty(JcrConstants.JCR_UUID, UserProvider.getContentID("testUser"));
                userTree.setProperty(UserConstants.REP_PRINCIPAL_NAME, "testUser");
                root.commit();
                fail("Invalid hierarchy should be detected");

            } catch (CommitFailedException e) {
                // success
            } finally {
                root.refresh();
            }
        }
    }

}