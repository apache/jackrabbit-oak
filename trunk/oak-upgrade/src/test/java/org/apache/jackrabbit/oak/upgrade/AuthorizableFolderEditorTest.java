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
package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.InputStream;

import static org.junit.Assert.assertThat;

public class AuthorizableFolderEditorTest extends AbstractRepositoryUpgradeTest {

    // this repository config sets the groupsPath and usersPath to match
    // this tests expectations
    private static final String REPOSITORY_XML_FILE = "repository-groupmember.xml";

    private static final String TEST_GROUP = "AuthorizableFolderEditorTest-Group";

    private static final String TEST_USER = "AuthorizableFolderEditorTest-User";

    private static final String HOME_PATH = "/home";

    private static final String GROUPS_PATH = HOME_PATH + "/groups";

    private static final String USERS_PATH = HOME_PATH + "/users";

    private static final String CONTROL_PATH = HOME_PATH + "/control";

    @Override
    protected void createSourceContent(final Session session) throws Exception {
        UserManager userMgr = ((JackrabbitSession) session).getUserManager();
        userMgr.autoSave(false);
        Group group = userMgr.createGroup(TEST_GROUP);
        User user = userMgr.createUser(TEST_USER, "secret");
        group.addMember(user);
        session.save();

        // simulate the error, set node types to incorrect values
        Node home = session.getNode("/home");
        home.setPrimaryType(JcrConstants.NT_UNSTRUCTURED);
        home.getNode("users").setPrimaryType(JcrConstants.NT_UNSTRUCTURED);
        home.getNode("groups").setPrimaryType(JcrConstants.NT_UNSTRUCTURED);
        home.addNode("control", JcrConstants.NT_UNSTRUCTURED);
        session.save();
    }

    @Override
    public InputStream getRepositoryConfig() {
        return getClass().getClassLoader().getResourceAsStream(REPOSITORY_XML_FILE);
    }

    @Test
    public void verifyCorrectedNodeTypes() throws RepositoryException {
        final Session session = createAdminSession();
        assertExisting(session, HOME_PATH, USERS_PATH, GROUPS_PATH, CONTROL_PATH);

        assertThat(session.getNode(HOME_PATH), hasNodeType(UserConstants.NT_REP_AUTHORIZABLE_FOLDER));
        assertThat(session.getNode(USERS_PATH), hasNodeType(UserConstants.NT_REP_AUTHORIZABLE_FOLDER));
        assertThat(session.getNode(GROUPS_PATH), hasNodeType(UserConstants.NT_REP_AUTHORIZABLE_FOLDER));
        assertThat(session.getNode(CONTROL_PATH), hasNodeType(JcrConstants.NT_UNSTRUCTURED));
    }

    private static Matcher<? super Node> hasNodeType(final String expectedNodeType) {
        return new TypeSafeMatcher<Node>() {

            private String path;

            @Override
            protected boolean matchesSafely(final Node node) {
                path = getPath(node);
                return getNodeTypeName(node).equals(expectedNodeType);
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("the node " + path + " to be of type ").appendValue(expectedNodeType);
            }

            @Override
            protected void describeMismatchSafely(final Node node, final Description mismatchDescription) {
                mismatchDescription.appendText(" was ").appendValue(getNodeTypeName(node));
            }

            private String getNodeTypeName(final Node node) {
                try {
                    return node.getPrimaryNodeType().getName();
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            }

            private String getPath(final Node node) {
                try {
                    return node.getPath();
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
