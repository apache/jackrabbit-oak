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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.EmptyPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.NT_REP_ACL;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_POLICY;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrincipalBasedPermissionProviderRegularTreePermissionTest extends AbstractPrincipalBasedTest {


    private PrincipalBasedPermissionProvider permissionProvider;
    private String childPath;

    @Before
    public void before() throws Exception {
        super.before();
        
        namePathMapper = NamePathMapper.DEFAULT;

        childPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 2);

        setupContentTrees(TEST_OAK_PATH);
        setupContentTrees(NT_FOLDER, childPath + "/folder");

        // Creates access control node
        setupAccessControlTree(childPath + "/accessControlledFolder");
        root.commit();
    }

    @Test
    public void testReadContent() throws Exception {
        setPermissions(getTestSystemUser().getPrincipal(), childPath, JCR_READ);
        permissionProvider = createPermissionProvider(root, getTestSystemUser().getPrincipal());
        try (ContentSession testSession = getSession(getTestSystemUser())) {
            Tree tree = testSession.getLatestRoot().getTree(childPath);
            assertTrue(getTreePermission(tree, permissionProvider).canRead());
        }
    }

    @Test
    public void testReadProperties() throws Exception {
        setPermissions(getTestSystemUser().getPrincipal(), childPath, JCR_ALL);
        permissionProvider = createPermissionProvider(root, getTestSystemUser().getPrincipal());
        try (ContentSession testSession = getSession(getTestSystemUser())) {
            Tree tree = testSession.getLatestRoot().getTree(childPath);
            assertTrue(getTreePermission(tree, permissionProvider).canReadProperties());
            assertTrue(getTreePermission(tree, permissionProvider).canReadAll());
        }
    }

    @Test
    public void testReadAccessControlNodes() throws Exception {
        setPermissions(getTestSystemUser().getPrincipal(), childPath + "/accessControlledFolder", JCR_READ_ACCESS_CONTROL);
        permissionProvider = createPermissionProvider(root, getTestSystemUser().getPrincipal());
        try (ContentSession testSession = getSession(getTestSystemUser())) {
            Tree tree = testSession.getLatestRoot().getTree(childPath + "/accessControlledFolder");
            TreePermission treePermission = getTreePermission(tree, permissionProvider);
            assertFalse(treePermission.canRead());
            TreePermission versionTreePermission = permissionProvider.getTreePermission(tree.getChild(REP_POLICY), treePermission);
            assertTrue(versionTreePermission.canRead());
        }
    }

    @Test
    public void testReadContentWithRestrictions() throws Exception {
        setPermissions(getTestSystemUser().getPrincipal(), childPath, JCR_READ);
        addRestrictionToPrincipal(getTestSystemUser().getPrincipal());
        permissionProvider = createPermissionProvider(root, getTestSystemUser().getPrincipal());
        try (ContentSession testSession = getSession(getTestSystemUser())) {
            Tree tree = testSession.getLatestRoot().getTree(childPath);
            TreePermission childPathPermission = getTreePermission(tree, permissionProvider);
            assertTrue(childPathPermission.canRead());
            assertFalse(childPathPermission.canReadProperties());
            assertFalse(childPathPermission.canReadAll());
            TreePermission folderPermission = permissionProvider.getTreePermission(tree.getChild("folder"), childPathPermission);
            assertTrue(folderPermission.canRead());
            assertFalse(folderPermission.canReadProperties());
        }        
    }
    
    // ------------------------------------------------< private >---------------------------------------------------
    
    private void setPermissions(Principal principal, String path, String... privileges) throws Exception {
        String jcrPath = getNamePathMapper().getJcrPath(path);
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(principal, jcrPath, privileges);
        addPrincipalBasedEntry(policy, jcrPath, privileges);
        root.commit();
    }

    private void addRestrictionToPrincipal(Principal testPrincipal) throws Exception {
        // add an entry with nt-name restriction at childPath allowing only to access a node with a specific name
        JackrabbitAccessControlManager accessControlManager = getAccessControlManager(root);
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, accessControlManager);
        Map<String, Value[]> restrictions = Map.of(REP_NT_NAMES, new Value[] {getValueFactory(root).createValue("folder", PropertyType.NAME)});
        policy.addEntry(childPath, privilegesFromNames(JCR_READ), Map.of(), restrictions);
        accessControlManager.setPolicy(policy.getPath(), policy);
        root.commit();
    }

    private void setupAccessControlTree(String path) throws Exception {
        // Creates given path as folder and creates rep:policy node setting as well all other required properties 
        // as orderable child nodes and setting the rep:accessControllable mixin
        setupContentTrees(NT_FOLDER, path);
        Tree accessControlFolder = root.getTree(path);
        TreeUtil.addMixin(accessControlFolder, MIX_REP_ACCESS_CONTROLLABLE,
                root.getTree(NodeTypeConstants.NODE_TYPES_PATH), root.getContentSession().getAuthInfo().getUserID());
        setupContentTrees(NT_REP_ACL, path + "/" + REP_POLICY);
        accessControlFolder.setOrderableChildren(true);
        accessControlFolder.getChild(REP_POLICY).setOrderableChildren(true);
    }

    private TreePermission getTreePermission(Tree root, PrincipalBasedPermissionProvider provider) {
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