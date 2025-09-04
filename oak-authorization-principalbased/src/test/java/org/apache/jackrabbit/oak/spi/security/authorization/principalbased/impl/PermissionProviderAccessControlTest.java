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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Value;
import java.security.Principal;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionProviderAccessControlTest extends AbstractPrincipalBasedTest {

    private Principal testPrincipal;
    private PrincipalBasedPermissionProvider permissionProvider;

    private String contentPath;
    private String childPath;

    private String child2Path;

    private String accessControlledPath;

    @Before
    public void before() throws Exception {
        super.before();

        testPrincipal = getTestSystemUser().getPrincipal();
        setupContentTrees(TEST_OAK_PATH);
        setupContentTrees("/oak:content/child2/grandchild2");

        contentPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 3);
        childPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 2);

        child2Path = "/oak:content/child2";

        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(testPrincipal, getNamePathMapper().getJcrPath(contentPath), JCR_READ);
        addPrincipalBasedEntry(policy, getNamePathMapper().getJcrPath(childPath), PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        addPrincipalBasedEntry(policy, getNamePathMapper().getJcrPath(child2Path), PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL);
        accessControlledPath = policy.getOakPath();
        root.commit();

        permissionProvider = createPermissionProvider(root, testPrincipal);
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    @Test
    public void testGetTreePermission() {
        String oakPath = PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY);
        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(oakPath)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertTrue(tp instanceof AbstractTreePermission);
        assertSame(TreeType.ACCESS_CONTROL, ((AbstractTreePermission) tp).getType());
    }

    @Test
    public void testIsGrantedOnAccessControlledTree() throws Exception {
        Tree accessControlledTree = root.getTree(getNamePathMapper().getOakPath(accessControlledPath));
        assertFalse(permissionProvider.isGranted(accessControlledTree, null, Permissions.READ));
        assertFalse(permissionProvider.isGranted(accessControlledTree, null, Permissions.READ_ACCESS_CONTROL));

        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ);
        root.commit();
        permissionProvider.refresh();

        assertTrue(permissionProvider.isGranted(accessControlledTree, null, Permissions.READ));

    }

    @Test
    public void testIsGrantedOnPolicyTree() {
        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.READ));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.READ_ACCESS_CONTROL));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.WRITE));
    }

    @Test
    public void testIsGrantedOnPolicyTreePrincipalReadable() throws Exception {
        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ);
        root.commit();
        permissionProvider.refresh();

        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        assertTrue(permissionProvider.isGranted(policyTree, null, Permissions.READ));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.READ_ACCESS_CONTROL));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.WRITE));
    }

    @Test
    public void testIsGrantedOnPolicyTreePrincipalAccessControlReadable() throws Exception {
        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ, JCR_READ_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        assertTrue(permissionProvider.isGranted(policyTree, null, Permissions.READ));
        assertTrue(permissionProvider.isGranted(policyTree, null, Permissions.READ_ACCESS_CONTROL));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL));
        assertFalse(permissionProvider.isGranted(policyTree, null, Permissions.WRITE));
    }

    @Test
    public void testIsGrantedOnEntryTree() throws Exception {
        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ, JCR_READ_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        for (Tree child : policyTree.getChildren()) {
            assertTrue(permissionProvider.isGranted(child, null, Permissions.READ|Permissions.READ_ACCESS_CONTROL));
            assertFalse(permissionProvider.isGranted(child, null, Permissions.MODIFY_ACCESS_CONTROL));
        }
    }

    @Test
    public void testIsGrantedOnEntryTreeAccessControlModifiable() throws Exception {
        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        for (Tree child : policyTree.getChildren()) {
            assertTrue(permissionProvider.isGranted(child, null, Permissions.READ));

            String effectivePath = child.getProperty(REP_EFFECTIVE_PATH).getValue(STRING);
            if (contentPath.equals(effectivePath)) {
                assertFalse(permissionProvider.isGranted(child, null, Permissions.READ_ACCESS_CONTROL));
                assertFalse(permissionProvider.isGranted(child, null, Permissions.MODIFY_ACCESS_CONTROL));
            } else if (childPath.equals(effectivePath)) {
                assertFalse(permissionProvider.isGranted(child, null, Permissions.READ_ACCESS_CONTROL));
                assertFalse(permissionProvider.isGranted(child, null, Permissions.MODIFY_ACCESS_CONTROL));
            } else if (child2Path.equals(effectivePath)) {
                assertFalse(permissionProvider.isGranted(child, null, Permissions.READ_ACCESS_CONTROL));
                assertTrue(permissionProvider.isGranted(child, null, Permissions.MODIFY_ACCESS_CONTROL));
            }
        }
    }

    @Test
    public void testIsGrantedOnEntryTreeAccessMgt() throws Exception {
        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        for (Tree child : policyTree.getChildren()) {
            assertTrue(permissionProvider.isGranted(child, null, Permissions.READ|Permissions.READ_ACCESS_CONTROL));
            assertTrue(permissionProvider.isGranted(child, child.getProperty(REP_EFFECTIVE_PATH), Permissions.READ_ACCESS_CONTROL));
            assertTrue(permissionProvider.isGranted(child, child.getProperty(REP_PRIVILEGES), Permissions.READ_ACCESS_CONTROL));

            String effectivePath = child.getProperty(REP_EFFECTIVE_PATH).getValue(STRING);
            if (contentPath.equals(effectivePath)) {
                assertFalse(permissionProvider.isGranted(child, null, Permissions.MODIFY_ACCESS_CONTROL));
            } else if (childPath.equals(effectivePath)) {
                assertFalse(permissionProvider.isGranted(child, null, Permissions.MODIFY_ACCESS_CONTROL));
            } else if (child2Path.equals(effectivePath)) {
                assertTrue(permissionProvider.isGranted(child, null, Permissions.MODIFY_ACCESS_CONTROL));
            }
        }
    }

    @Test
    public void testIsGrantedOnNonExistingRestrictionTree() throws Exception {
        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        for (Tree child : policyTree.getChildren()) {
            Tree restr = child.getChild(REP_RESTRICTIONS);
            PropertyState propertyState = PropertyStates.createProperty(REP_GLOB, "any");

            assertFalse(permissionProvider.isGranted(restr, null, Permissions.READ));

            String effectivePath = child.getProperty(REP_EFFECTIVE_PATH).getValue(STRING);
            if (contentPath.equals(effectivePath)) {
                assertTrue(permissionProvider.isGranted(restr, null, Permissions.READ_ACCESS_CONTROL));
                assertTrue(permissionProvider.isGranted(restr, propertyState, Permissions.READ_ACCESS_CONTROL));
                assertFalse(permissionProvider.isGranted(restr, null, Permissions.MODIFY_ACCESS_CONTROL));
            } else if (childPath.equals(effectivePath)) {
                assertTrue(permissionProvider.isGranted(restr, null, Permissions.READ_ACCESS_CONTROL));
                assertTrue(permissionProvider.isGranted(restr, propertyState, Permissions.READ_ACCESS_CONTROL));
                assertFalse(permissionProvider.isGranted(restr, null, Permissions.MODIFY_ACCESS_CONTROL));
            } else if (child2Path.equals(effectivePath)) {
                assertTrue(permissionProvider.isGranted(restr, null, Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL));
                assertTrue(permissionProvider.isGranted(restr, propertyState, Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL));
            }
        }
    }

    @Test
    public void testIsGrantedOnRestrictionTree() throws Exception {
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, getAccessControlManager(root));
        Map<String, Value> restr = Map.of(getNamePathMapper().getJcrName(REP_GLOB), getValueFactory(root).createValue(REP_RESTRICTIONS + "*"));
        policy.addEntry(accessControlledPath, privilegesFromNames(JCR_READ_ACCESS_CONTROL), restr, Map.of());
        root.commit();
        permissionProvider.refresh();

        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        for (Tree child : policyTree.getChildren()) {
            assertFalse(permissionProvider.isGranted(child, null, Permissions.READ));
            if (child.hasChild(REP_RESTRICTIONS)) {
                Tree restrTree = child.getChild(REP_RESTRICTIONS);
                assertTrue(permissionProvider.isGranted(restrTree, null, Permissions.READ_ACCESS_CONTROL));
                assertFalse(permissionProvider.isGranted(restrTree, null, Permissions.READ));
                assertFalse(permissionProvider.isGranted(restrTree, null, Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL));
                for (PropertyState ps : restrTree.getProperties()) {
                    assertTrue(permissionProvider.isGranted(restrTree, ps, Permissions.READ_ACCESS_CONTROL));
                }
                break;
            }
        }
    }

    @Test
    public void testIsGrantedByPath() throws Exception {
        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        assertTrue(permissionProvider.isGranted(accessControlledPath, Permissions.getString(Permissions.READ|Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL)));
        assertFalse(permissionProvider.isGranted(accessControlledPath, Permissions.getString(Permissions.READ|Permissions.WRITE)));

        String policyPath = PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY);
        assertTrue(permissionProvider.isGranted(policyPath, Permissions.getString(Permissions.READ_PROPERTY|Permissions.READ_NODE|Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL)));

        for (Tree child : root.getTree(policyPath).getChildren()) {
            String childPath = child.getPath();
            String effectivePath = child.getProperty(REP_EFFECTIVE_PATH).getValue(STRING);
            if (contentPath.equals(effectivePath)) {
                assertTrue(permissionProvider.isGranted(childPath, Permissions.getString(Permissions.READ|Permissions.READ_ACCESS_CONTROL)));
                assertFalse(permissionProvider.isGranted(childPath, Permissions.getString(Permissions.MODIFY_ACCESS_CONTROL)));
            } else if (childPath.equals(effectivePath)) {
                assertTrue(permissionProvider.isGranted(childPath, Permissions.getString(Permissions.READ|Permissions.READ_ACCESS_CONTROL)));
                assertFalse(permissionProvider.isGranted(childPath, Permissions.getString(Permissions.READ|Permissions.MODIFY_ACCESS_CONTROL)));
            } else if (child2Path.equals(effectivePath)) {
                assertTrue(permissionProvider.isGranted(childPath, Permissions.getString(Permissions.READ|Permissions.READ_ACCESS_CONTROL)));
                assertTrue(permissionProvider.isGranted(childPath, Permissions.getString(Permissions.READ|Permissions.MODIFY_ACCESS_CONTROL)));
            }
        }
    }

    @Test
    public void testGetPrivileges() throws Exception {
        assertTrue(permissionProvider.getPrivileges(root.getTree(accessControlledPath)).isEmpty());
        Tree policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        assertTrue(permissionProvider.getPrivileges(policyTree).isEmpty());
        for (Tree child : policyTree.getChildren()) {
            assertTrue(permissionProvider.getPrivileges(child).isEmpty());
        }

        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ);
        root.commit();
        permissionProvider.refresh();

        Set<String> expectedPrivNames = Set.of(JCR_READ);
        assertEquals(expectedPrivNames, permissionProvider.getPrivileges(root.getTree(accessControlledPath)));
        policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        assertEquals(expectedPrivNames, permissionProvider.getPrivileges(policyTree));
        for (Tree child : policyTree.getChildren()) {
            assertEquals(expectedPrivNames, permissionProvider.getPrivileges(child));
        }

        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        expectedPrivNames = Set.of(JCR_READ, JCR_READ_ACCESS_CONTROL);
        assertEquals(expectedPrivNames, permissionProvider.getPrivileges(root.getTree(accessControlledPath)));
        policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        assertEquals(expectedPrivNames, permissionProvider.getPrivileges(policyTree));
        for (Tree child : policyTree.getChildren()) {
            assertEquals(Set.of(JCR_READ, JCR_READ_ACCESS_CONTROL), permissionProvider.getPrivileges(child));
        }

        setupPrincipalBasedAccessControl(testPrincipal, accessControlledPath, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        expectedPrivNames = Set.of(JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        assertEquals(expectedPrivNames, permissionProvider.getPrivileges(root.getTree(accessControlledPath)));
        policyTree = root.getTree(PathUtils.concat(accessControlledPath, REP_PRINCIPAL_POLICY));
        assertEquals(expectedPrivNames, permissionProvider.getPrivileges(policyTree));
        for (Tree child : policyTree.getChildren()) {
            String effectivePath = child.getProperty(REP_EFFECTIVE_PATH).getValue(STRING);
            if (contentPath.equals(effectivePath)) {
                assertEquals(Set.of(JCR_READ, JCR_READ_ACCESS_CONTROL), permissionProvider.getPrivileges(child));
            } else if (childPath.equals(effectivePath)) {
                assertEquals(Set.of(JCR_READ, JCR_READ_ACCESS_CONTROL), permissionProvider.getPrivileges(child));
            } else if (child2Path.equals(effectivePath)) {
                assertEquals(Set.of(JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL), permissionProvider.getPrivileges(child));
            }
        }
    }
}