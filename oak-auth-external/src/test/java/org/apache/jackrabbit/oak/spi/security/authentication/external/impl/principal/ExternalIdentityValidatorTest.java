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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExternalIdentityValidatorTest extends ExternalLoginTestBase {

    String testUserPath;
    String externalUserPath;

    @Override
    public void before() throws Exception {
        super.before();

        testUserPath = getTestUser().getPath();

        // force an external user to be synchronized into the repo
        login(new SimpleCredentials(USER_ID, new char[0])).close();
        root.refresh();

        Authorizable a = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(a);
        assertEquals(isDynamic(), a.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES));
        externalUserPath = a.getPath();
    }

    @Override
    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.user().setDynamicMembership(isDynamic());
        return config;
    }

    protected boolean isDynamic() {
        return true;
    }

    @Test(expected = CommitFailedException.class)
    public void testAddExternalPrincipalNames() throws Exception {
        Tree userTree = root.getTree(testUserPath);
        try {
            userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, ImmutableList.of("principalName"), Type.STRINGS);
            root.commit();
            fail("Creating rep:externalPrincipalNames must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertException(e, CONSTRAINT, 70);
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddExternalPrincipalNamesAsSystemMissingExternalId() throws Exception {
        Root systemRoot = getSystemRoot();
        try {
            Tree userTree = systemRoot.getTree(testUserPath);
            userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, ImmutableList.of("principalName"), Type.STRINGS);
            systemRoot.commit();
            fail("Creating rep:externalPrincipalNames without rep:externalId must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertException(e, CONSTRAINT, 72);
        } finally {
            systemRoot.refresh();
        }
    }

    @Test
    public void testAddExternalPrincipalNamesAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        Tree userTree = systemRoot.getTree(testUserPath);
        userTree.setProperty(REP_EXTERNAL_ID, "externalId");
        userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, ImmutableList.of("principalName"), Type.STRINGS);
        systemRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveExternalPrincipalNames() throws Exception {
        Tree userTree = root.getTree(externalUserPath);
        try {
            userTree.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
            root.commit();
            fail("Removing rep:externalPrincipalNames must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertException(e, CONSTRAINT, 70);
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testRemoveExternalPrincipalNamesAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        Tree userTree = systemRoot.getTree(externalUserPath);

        // removal with system root must succeed
        userTree.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
        systemRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testModifyExternalPrincipalNames() throws Exception {
        Tree userTree = root.getTree(externalUserPath);
        try {
            userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, List.of("principalNames"), Type.STRINGS);
            root.commit();
            fail("Changing rep:externalPrincipalNames must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertException(e, CONSTRAINT, 70);
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testModifyExternalPrincipalNamesAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        Tree userTree = systemRoot.getTree(externalUserPath);

        // changing with system root must succeed
        userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, ImmutableList.of("principalNames"), Type.STRINGS);
        systemRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testExternalPrincipalNamesType() throws Exception {
        Root systemRoot = getSystemRoot();
        Tree userTree = systemRoot.getTree(testUserPath);

        java.util.Map<Type, Object> valMap = ImmutableMap.of(
                Type.BOOLEANS, Set.of(Boolean.TRUE),
                Type.LONGS, Set.of(1234L),
                Type.NAMES, Set.of("id", "id2")
        );
        for (Type t : valMap.keySet()) {
            Object val = valMap.get(t);
            try {
                userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, val, t);
                systemRoot.commit();
                fail("Creating rep:externalPrincipalNames with type "+t+" must be detected.");
            } catch (CommitFailedException e) {
                // success
                assertException(e, CONSTRAINT, 71);
            } finally {
                systemRoot.refresh();
            }
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testExternalPrincipalNamesSingle() throws Exception {
        Root systemRoot = getSystemRoot();
        try {
            Tree userTree = systemRoot.getTree(testUserPath);
            userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, "id");
            systemRoot.commit();
            fail("Creating rep:externalPrincipalNames as single STRING property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertException(e, CONSTRAINT, 71);
        } finally {
            systemRoot.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testExternalPrincipalNamesArrayMismatch() throws Exception {
        NodeState ns = mock(NodeState.class);
        PropertyState ps = when(mock(PropertyState.class).getName()).thenReturn(REP_EXTERNAL_PRINCIPAL_NAMES).getMock();
        Type type = Type.STRINGS;
        when(ps.getType()).thenReturn(type);
        when(ps.isArray()).thenReturn(false);

        try {
            Validator v = new ExternalIdentityValidatorProvider(true, true).getRootValidator(ns, ns, null);
            v.propertyAdded(ps);
        } catch (CommitFailedException e) {
            assertException(e, CONSTRAINT, 71);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRepExternalIdMultiple() throws Exception {
        Root systemRoot = getSystemRoot();
        try {
            Tree userTree = systemRoot.getTree(testUserPath);
            userTree.setProperty(REP_EXTERNAL_ID, ImmutableList.of("id", "id2"), Type.STRINGS);
            systemRoot.commit();
            fail("Creating rep:externalId as multiple STRING property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertException(e, CONSTRAINT, 75);
        } finally {
            systemRoot.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRepExternalIdType() throws Exception {
        Root systemRoot = getSystemRoot();
        Tree userTree = systemRoot.getTree(testUserPath);

        java.util.Map<Type, Object> valMap = ImmutableMap.of(
                Type.BOOLEAN, Boolean.TRUE,
                Type.LONG, 1234L,
                Type.NAME, "id"
        );
        for (Type t : valMap.keySet()) {
            Object val = valMap.get(t);
            try {
                userTree.setProperty(REP_EXTERNAL_ID, val, t);
                systemRoot.commit();
                fail("Creating rep:externalId with type "+t+" must be detected.");
            } catch (CommitFailedException e) {
                // success
                assertException(e, CONSTRAINT, 75);
            } finally {
                systemRoot.refresh();
            }
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRepExternalIdTypeArrayMismatch() throws Exception {
        NodeState ns = mock(NodeState.class);
        PropertyState ps = when(mock(PropertyState.class).getName()).thenReturn(REP_EXTERNAL_ID).getMock();
        Type type = Type.STRING;
        when(ps.getType()).thenReturn(type);
        when(ps.isArray()).thenReturn(true);

        try {
            Validator v = new ExternalIdentityValidatorProvider(true, true).getRootValidator(ns, ns, null);
            v.propertyAdded(ps);
        } catch (CommitFailedException e) {
            assertException(e, CONSTRAINT, 75);
        }
    }

    @Test
    public void testCreateUserWithRepExternalId() throws Exception {
        User u = getUserManager(root).createUser(TestIdentityProvider.ID_SECOND_USER, null);
        root.getTree(u.getPath()).setProperty(REP_EXTERNAL_ID, TestIdentityProvider.ID_SECOND_USER);
        root.commit();
    }

    @Test
    public void testCreateUserWithRepExternalIdAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        User u = getUserManager(systemRoot).createUser(TestIdentityProvider.ID_SECOND_USER, null);
        systemRoot.getTree(u.getPath()).setProperty(REP_EXTERNAL_ID, TestIdentityProvider.ID_SECOND_USER);
        systemRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testAddRepExternalId() throws Exception {
        try {
            root.getTree(testUserPath).setProperty(REP_EXTERNAL_ID, "id");
            root.commit();
            fail("Adding rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertException(e, CONSTRAINT, 74);
        }

    }

    @Test
    public void testAddRepExternalIdAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        systemRoot.getTree(testUserPath).setProperty(REP_EXTERNAL_ID, "id");
        systemRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testModifyRepExternalId() throws Exception {
        try {
            root.getTree(externalUserPath).setProperty(REP_EXTERNAL_ID, "anotherValue");
            root.commit();

            fail("Modification of rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertException(e, CONSTRAINT, 74);
        }
    }

    @Test
    public void testModifyRepExternalIdAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        systemRoot.getTree(externalUserPath).setProperty(REP_EXTERNAL_ID, "anotherValue");
        systemRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveRepExternalId() throws Exception {
        try {
            root.getTree(externalUserPath).removeProperty(REP_EXTERNAL_ID);
            root.commit();

            fail("Removal of rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertException(e, CONSTRAINT, 73);
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveRepExternalIdAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        try {
            Tree userTree = systemRoot.getTree(externalUserPath);
            userTree.removeProperty(REP_EXTERNAL_ID);
            systemRoot.commit();
            fail("Removing rep:externalId is not allowed if rep:externalPrincipalNames is present.");
        } catch (CommitFailedException e) {
            // success
            assertException(e, CONSTRAINT, 73);
        } finally {
            systemRoot.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveRepExternalIdWithoutPrincipalNames() throws Exception {
        Root systemRoot = getSystemRoot();
        systemRoot.getTree(testUserPath).setProperty(REP_EXTERNAL_ID, "id");
        systemRoot.commit();
        root.refresh();

        try {
            root.getTree(testUserPath).removeProperty(REP_EXTERNAL_ID);
            root.commit();

            fail("Removal of rep:externalId must be detected in the default setup.");
        } catch (CommitFailedException e) {
            // success: verify nature of the exception
            assertException(e, CONSTRAINT, 74);
        }
    }

    @Test
    public void testRemoveRepExternalIdWithoutPrincipalNamesAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        systemRoot.getTree(testUserPath).setProperty(REP_EXTERNAL_ID, "id");
        systemRoot.commit();

        systemRoot.getTree(testUserPath).removeProperty(REP_EXTERNAL_ID);
        systemRoot.commit();
    }

    @Test
    public void testRemoveExternalUser() throws Exception {
        getUserManager(root).getAuthorizable(USER_ID).remove();
        root.commit();
    }

    @Test
    public void testRemoveExternalUserTree() throws Exception {
        root.getTree(externalUserPath).remove();
        root.commit();
    }
}