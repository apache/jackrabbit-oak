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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModuleTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExternalIdentityValidatorTest extends ExternalLoginModuleTestBase {

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
        assertTrue(a.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES));
        externalUserPath = a.getPath();
    }

    @Override
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.user().setDynamicMembership(true);
        return config;
    }

    @Test
    public void testRemoveRepExternalIdAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        try {
            NodeUtil n = new NodeUtil(systemRoot.getTree(externalUserPath));

            n.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_ID);
            systemRoot.commit();
            fail("Removing rep:externalId is not allowed if rep:externalPrincipalNames is present.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(73, e.getCode());
        } finally {
            systemRoot.refresh();
        }
    }

    @Test
    public void testAddExternalPrincipalNames() throws Exception {
        Tree userTree = root.getTree(testUserPath);
        NodeUtil userNode = new NodeUtil(userTree);
        try {
            userNode.setStrings(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, "principalName");
            root.commit();
            fail("Creating rep:externalPrincipalNames must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(70, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testAddExternalPrincipalNamesAsSystemMissingExternalId() throws Exception {
        Root systemRoot = getSystemRoot();
        try {
            NodeUtil n = new NodeUtil(systemRoot.getTree(testUserPath));
            n.setStrings(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, "principalName");
            systemRoot.commit();
            fail("Creating rep:externalPrincipalNames without rep:externalId must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(72, e.getCode());
        } finally {
            systemRoot.refresh();
        }
    }

    @Test
    public void testAddExternalPrincipalNamesAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        NodeUtil n = new NodeUtil(systemRoot.getTree(testUserPath));
        n.setString(ExternalIdentityConstants.REP_EXTERNAL_ID, "externalId");
        n.setStrings(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, "principalName");
        systemRoot.commit();
    }

    @Test
    public void testRemoveExternalPrincipalNames() throws Exception {
        Tree userTree = root.getTree(externalUserPath);
        try {
            userTree.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
            root.commit();
            fail("Removing rep:externalPrincipalNames must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(70, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testRemoveExternalPrincipalNamesAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        NodeUtil n = new NodeUtil(systemRoot.getTree(externalUserPath));

        // removal with system root must succeed
        n.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
        systemRoot.commit();
    }

    @Test
    public void testModifyExternalPrincipalNames() throws Exception {
        Tree userTree = root.getTree(externalUserPath);
        try {
            userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, Lists.newArrayList("principalNames"), Type.STRINGS);
            root.commit();
            fail("Changing rep:externalPrincipalNames must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(70, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testModifyExternalPrincipalNamesAsSystem() throws Exception {
        Root systemRoot = getSystemRoot();
        NodeUtil n = new NodeUtil(systemRoot.getTree(externalUserPath));

        // changing with system root must succeed
        n.setStrings(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, "principalNames");
        systemRoot.commit();
    }

    @Test
    public void testExternalPrincipalNamesType() throws Exception {
        Root systemRoot = getSystemRoot();
        Tree userTree = systemRoot.getTree(testUserPath);

        java.util.Map<Type, Object> valMap = ImmutableMap.<Type, Object>of(
                Type.BOOLEANS, ImmutableSet.of(Boolean.TRUE),
                Type.LONGS, ImmutableSet.of(new Long(1234)),
                Type.NAMES, ImmutableSet.of("id", "id2")
        );
        for (Type t : valMap.keySet()) {
            Object val = valMap.get(t);
            try {
                userTree.setProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, val, t);
                systemRoot.commit();
                fail("Creating rep:externalPrincipalNames with type "+t+" must be detected.");
            } catch (CommitFailedException e) {
                // success
                assertEquals(71, e.getCode());
            } finally {
                systemRoot.refresh();
            }
        }
    }

    @Test
    public void testExternalPrincipalNamesSingle() throws Exception {
        Root systemRoot = getSystemRoot();
        try {
            NodeUtil n = new NodeUtil(systemRoot.getTree(testUserPath));
            n.setString(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES, "id");
            systemRoot.commit();
            fail("Creating rep:externalPrincipalNames as single STRING property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals(71, e.getCode());
        } finally {
            systemRoot.refresh();
        }
    }
}