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

package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;


import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrincipalBasedPermissionProviderRegularTreePermissionTest extends AbstractPrincipalBasedTest {

    private PrincipalBasedPermissionProvider permissionProvider;

    private String childPath;

    private User alternativeServiceUser;
    private PrincipalBasedPermissionProvider alternatePermissionProvider;

    @Before
    public void before() throws Exception {
        super.before();

        childPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 2);

        Principal testPrincipal = getTestSystemUser().getPrincipal();

        setupContentTrees(TEST_OAK_PATH);
        setupContentTrees(NT_FOLDER, childPath + "/folder", TEST_OAK_PATH + "/folder");

        createPrincipalPolicy(testPrincipal);
        addRestrictionToPrincipal(testPrincipal);

        Principal alternativePrincipal = getAlternativeSystemUser().getPrincipal();
        createPrincipalPolicy(alternativePrincipal);

        root.commit();

        permissionProvider = createPermissionProvider(root, testPrincipal);
        alternatePermissionProvider = createPermissionProvider(root, alternativePrincipal);
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    @Test
    public void testTreePermissionWithRestriction() throws Exception {
        try (ContentSession testSession = getSession(getTestSystemUser())) {
            Tree tree = testSession.getLatestRoot().getTree(childPath);
            assertFalse(permissionProvider.getTreePermission(tree, getParentTreePermission(tree.getParent(), permissionProvider)).canReadAll());
        }
    }

    @Test
    public void testTreePermissionWithoutRestriction() throws Exception {
        try (ContentSession testSession = getSession(alternativeServiceUser)) {
            Tree tree = testSession.getLatestRoot().getTree(childPath);
            assertTrue(alternatePermissionProvider.getTreePermission(tree, getParentTreePermission(tree.getParent(), alternatePermissionProvider)).canReadAll());
        }
    }


    //-------------------------------

    private User getAlternativeSystemUser() throws Exception {
        if (alternativeServiceUser == null) {
            String uid = "alternativeUser" + UUID.randomUUID();
            alternativeServiceUser = getUserManager(root).createSystemUser(uid, INTERMEDIATE_PATH);
            root.commit();
        }
        return alternativeServiceUser;
    }

    private void createPrincipalPolicy(Principal testPrincipal) throws Exception {
        // setup permissions on childPath + TEST_OAK_PATH
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(testPrincipal, getNamePathMapper().getJcrPath(childPath), JCR_READ, JCR_REMOVE_CHILD_NODES, JCR_READ_ACCESS_CONTROL);
        addPrincipalBasedEntry(policy, getNamePathMapper().getJcrPath(TEST_OAK_PATH), JCR_VERSION_MANAGEMENT);
    }

    private void addRestrictionToPrincipal(Principal testPrincipal) throws Exception {
        // add an entry with nt-name restriction at childPath allowing only to access a node with a specific name
        JackrabbitAccessControlManager accessControlManager = getAccessControlManager(root);
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, accessControlManager);
        Map<String, Value[]> restrictions = Map.of(REP_NT_NAMES, new Value[] {getValueFactory(root).createValue("folder", PropertyType.NAME)});
        policy.addEntry(childPath, privilegesFromNames(JCR_READ), Map.of(), restrictions);
        accessControlManager.setPolicy(policy.getPath(), policy);
    }

    private TreePermission getParentTreePermission(Tree root, PrincipalBasedPermissionProvider provider) {
        return new AbstractTreePermission(root, TreeType.DEFAULT) {
            @Override
            PrincipalBasedPermissionProvider getPermissionProvider() {
                return provider;
            }
        };
    }

    private ContentSession getSession(User user) throws Exception {
        Set<Principal> principals = Set.of(user.getPrincipal());
        AuthInfo authInfo = new AuthInfoImpl(user.getID(), Collections.emptyMap(), principals);
        Subject subject = new Subject(true, principals, Set.of(authInfo), Set.of());
        return Java23Subject.doAsPrivileged(subject, (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null);
    }
}