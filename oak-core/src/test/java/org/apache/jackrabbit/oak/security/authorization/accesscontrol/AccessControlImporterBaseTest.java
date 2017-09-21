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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public abstract class AccessControlImporterBaseTest  extends AbstractSecurityTest implements AccessControlConstants {

    final NodeInfo aceInfo = new NodeInfo("anyAceName", NT_REP_GRANT_ACE, ImmutableList.of(), null);
    final NodeInfo restrInfo = new NodeInfo("anyRestrName", NT_REP_RESTRICTIONS, ImmutableList.of(), null);
    final PropInfo unknownPrincipalInfo = new PropInfo(REP_PRINCIPAL_NAME, PropertyType.STRING, createTextValue("unknownPrincipal"));

    Tree accessControlledTree;
    Tree aclTree;

    AccessControlImporter importer;

    private String principalName;
    private PropInfo principalInfo;
    private PropInfo privInfo;

    @Override
    public void before() throws Exception {
        super.before();

        Tree t = root.getTree(PathUtils.ROOT_PATH).addChild("testNode");
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, t.getPath());
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy(t.getPath(), acl);
        root.commit();

        accessControlledTree = root.getTree("/testNode");
        aclTree = accessControlledTree.getChild(REP_POLICY);

        importer = new AccessControlImporter();

        principalName = getTestUser().getPrincipal().getName();
        principalInfo = new PropInfo(REP_PRINCIPAL_NAME, PropertyType.STRING, createTextValue(principalName));
        privInfo = new PropInfo(REP_PRIVILEGES, PropertyType.NAME, createTextValues(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES));
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            Tree t = root.getTree("/testNode");
            if (t.exists()) {
                t.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters params = ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior());
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, params);
    }

    abstract String getImportBehavior();

    Session mockJackrabbitSession() throws Exception {
        JackrabbitSession s = Mockito.mock(JackrabbitSession.class);
        when(s.getPrincipalManager()).thenReturn(getPrincipalManager(root));
        when(s.getAccessControlManager()).thenReturn(getAccessControlManager(root));
        return s;
    }

    boolean isWorkspaceImport() {
        return false;
    }

    boolean init() throws Exception {
        return importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider());
    }

    TextValue createTextValue(@Nonnull String val) {
        return new TextValue() {
            @Override
            public String getString() {
                return val;
            }

            @Override
            public Value getValue(int targetType) throws RepositoryException {
                return getValueFactory(root).createValue(val, targetType);
            }

            @Override
            public void dispose() {
                //nop

            }
        };
    }

    List<TextValue> createTextValues(@Nonnull String... values) {
        List<TextValue> l = new ArrayList();
        for (String v : values) {
            l.add(createTextValue(v));
        }
        return l;
    }

    //---------------------------------------------------------------< init >---
    @Test
    public void testInitNoJackrabbitSession() throws Exception {
        Session s = Mockito.mock(Session.class);
        assertFalse(importer.init(s, root, getNamePathMapper(), false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test(expected = IllegalStateException.class)
    public void testInitAlreadyInitialized() throws Exception {
        init();
        importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider());
    }

    @Test
    public void testInitImportUUIDBehaviorRemove() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }


    @Test
    public void testInitImportUUIDBehaviorReplace() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviorThrow() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviourCreateNew() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    //--------------------------------------------------------------< start >---
    @Test(expected = IllegalStateException.class)
    public void testStartNotInitialized() throws Exception {
        importer.start(Mockito.mock(Tree.class));
    }

    @Test
    public void testStartRootTree() throws Exception {
        init();
        assertFalse(importer.start(root.getTree(PathUtils.ROOT_PATH)));
    }

    @Test
    public void testStartAccessControlledTree() throws Exception {
        init();
        assertFalse(importer.start(accessControlledTree));
    }

    @Test
    public void testStartAclTree() throws Exception {
        init();
        assertTrue(importer.start(aclTree));
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testStartAclTreeMissingMixin() throws Exception {
        init();
        accessControlledTree.removeProperty(JcrConstants.JCR_MIXINTYPES);
        assertFalse(importer.start(aclTree));
    }

    @Test
    public void testStartRepoPolicyTree() throws Exception {
        init();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, null);
        acMgr.setPolicy(null, acl);

        Tree repoPolicy = root.getTree("/"+REP_REPO_POLICY);
        assertTrue(repoPolicy.exists());

        assertTrue(importer.start(repoPolicy));
    }

    @Test
    public void testStartRepoPolicyTreeMissingMixin() throws Exception {
        init();

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree repoPolicy = accessControlledTree.addChild(REP_REPO_POLICY);
        repoPolicy.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_ACL, Type.NAME);

        assertFalse(importer.start(repoPolicy));
    }

    @Test
    public void testStartMisplacedRepoPolicyTree() throws Exception {
        init();

        TreeUtil.addMixin(accessControlledTree, MIX_REP_REPO_ACCESS_CONTROLLABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        Tree repoPolicy = accessControlledTree.addChild(REP_REPO_POLICY);
        repoPolicy.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_ACL, Type.NAME);

        assertFalse(importer.start(repoPolicy));
    }

    //--------------------------------------------------< processReferences >---

    @Test
    public void testProcessReferencesIsNoOp() throws Exception {
        importer.processReferences();
        assertFalse(root.hasPendingChanges());
    }

    //-----------------------------------------------------< startChildInfo >---

    @Test(expected = IllegalStateException.class)
    public void testStartChildInfoNotInitialized() throws Exception {
        importer.startChildInfo(Mockito.mock(NodeInfo.class), ImmutableList.of());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoUnknownType() throws Exception {
        NodeInfo invalidChildInfo = new NodeInfo("anyName", NodeTypeConstants.NT_OAK_UNSTRUCTURED, ImmutableList.of(), null);
        init();
        importer.start(aclTree);
        importer.startChildInfo(invalidChildInfo, ImmutableList.of());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartNestedAceChildInfo() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of());
        importer.startChildInfo(aceInfo, ImmutableList.of());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartRestrictionChildInfoWithoutAce() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(restrInfo, ImmutableList.of());
    }

    public void testStartAceAndRestrictionChildInfo() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of());
        importer.startChildInfo(restrInfo, ImmutableList.of());
    }

    @Test(expected = AccessControlException.class)
    public void testStartAceChildInfoInvalidPrivilege() throws Exception {
        init();
        importer.start(aclTree);
        PropInfo invalidPrivInfo = new PropInfo(REP_PRIVILEGES, PropertyType.NAME, createTextValues("jcr:invalidPrivilege"), PropInfo.MultipleStatus.MULTIPLE);
        importer.startChildInfo(aceInfo, ImmutableList.of(invalidPrivInfo));
    }

    //-------------------------------------------------------< endChildInfo >---

    @Test(expected = IllegalStateException.class)
    public void testEndChildInfoNotInitialized() throws Exception {
        importer.endChildInfo();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEndChildInfoWithoutStart() throws Exception {
        init();
        importer.start(aclTree);
        importer.endChildInfo();
    }

    @Test(expected = AccessControlException.class)
    public void testEndChildInfoIncompleteAce() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of());
        importer.endChildInfo();
    }

    //----------------------------------------------------------------< end >---
    @Test(expected = IllegalStateException.class)
    public void testEndWithoutStart() throws Exception {
        importer.end(aclTree);
    }

    @Test(expected = IllegalStateException.class)
    public void testEndWithoutAcl() throws Exception {
        assertFalse(importer.start(accessControlledTree));
        importer.end(accessControlledTree);
    }

    @Test
    public void testEndWithoutChildInfo() throws Exception {
        init();
        importer.start(aclTree);
        importer.end(aclTree);

        assertTrue(root.hasPendingChanges());
        assertFalse(aclTree.getChildren().iterator().hasNext());
    }

    //------------------------------------------------< complete acl import >---
    @Test(expected = AccessControlException.class)
    public void testInvalidRestriction() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of(principalInfo, privInfo));

        PropInfo invalidRestrProp = new PropInfo(REP_GLOB, PropertyType.NAME, createTextValues("glob1", "glob2"), PropInfo.MultipleStatus.MULTIPLE);
        importer.startChildInfo(restrInfo, ImmutableList.of(invalidRestrProp));
        importer.endChildInfo();
        importer.endChildInfo();
        importer.end(aclTree);
    }

    @Test(expected = ValueFormatException.class)
    public void testUnknownRestriction() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of(principalInfo, privInfo));

        PropInfo invalidRestrProp = new PropInfo("unknown", PropertyType.STRING, createTextValue("val"));
        importer.startChildInfo(restrInfo, ImmutableList.of(invalidRestrProp));
        importer.endChildInfo();
        importer.endChildInfo();
        importer.end(aclTree);
    }

    @Test
    public void testImportSimple() throws Exception {
        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of(principalInfo, privInfo));
        importer.endChildInfo();
        importer.end(aclTree);

        assertTrue(aclTree.getChildren().iterator().hasNext());
        Tree aceTree = aclTree.getChildren().iterator().next();

        assertEquals(principalName, TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
        assertEquals(
                ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES),
                ImmutableSet.copyOf(TreeUtil.getNames(aceTree, REP_PRIVILEGES)));
        assertFalse(aceTree.hasChild(REP_RESTRICTIONS));
    }

    @Test
    public void testImportWithRestrictions() throws Exception {
        // single value restriction
        PropInfo globInfo = new PropInfo(REP_GLOB, PropertyType.STRING, createTextValue("/*"));
        // mv restriction
        PropInfo ntNamesInfo = new PropInfo(REP_NT_NAMES, PropertyType.NAME, createTextValues(NodeTypeConstants.NT_OAK_RESOURCE, NodeTypeConstants.NT_OAK_RESOURCE));
        // mv restriction with singular value
        PropInfo itemNamesInfo = new PropInfo(REP_ITEM_NAMES, PropertyType.NAME, createTextValue("itemName"));

        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of(principalInfo, privInfo, globInfo, ntNamesInfo, itemNamesInfo));
        importer.endChildInfo();
        importer.end(aclTree);

        assertImport(aclTree, principalName);
    }

    @Test
    public void testImportWithRestrictionNodeInfo() throws Exception {
        // single value restriction
        PropInfo globInfo = new PropInfo(REP_GLOB, PropertyType.STRING, createTextValue("/*"));
        // mv restriction
        PropInfo ntNamesInfo = new PropInfo(REP_NT_NAMES, PropertyType.NAME, createTextValues(NodeTypeConstants.NT_OAK_RESOURCE, NodeTypeConstants.NT_OAK_RESOURCE));
        // mv restriction with singular value
        PropInfo itemNamesInfo = new PropInfo(REP_ITEM_NAMES, PropertyType.NAME, createTextValue("itemName"));

        init();
        importer.start(aclTree);
        importer.startChildInfo(aceInfo, ImmutableList.of(principalInfo, privInfo));
        importer.startChildInfo(restrInfo, ImmutableList.of(globInfo, ntNamesInfo, itemNamesInfo));
        importer.endChildInfo();
        importer.endChildInfo();
        importer.end(aclTree);

        assertImport(aclTree, principalName);
    }

    private static void assertImport(@Nonnull Tree aclTree, @Nonnull String principalName) {
        assertTrue(aclTree.getChildren().iterator().hasNext());
        Tree aceTree = aclTree.getChildren().iterator().next();

        assertEquals(principalName, TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
        assertEquals(
                ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES),
                ImmutableSet.copyOf(TreeUtil.getNames(aceTree, REP_PRIVILEGES)));

        assertTrue(aceTree.hasChild(REP_RESTRICTIONS));

        Tree restrTree = aceTree.getChild(REP_RESTRICTIONS);
        assertEquals("/*", TreeUtil.getString(restrTree, REP_GLOB));
        assertEquals(Lists.newArrayList(NodeTypeConstants.NT_OAK_RESOURCE, NodeTypeConstants.NT_OAK_RESOURCE), restrTree.getProperty(REP_NT_NAMES).getValue(Type.NAMES));
        assertEquals(Lists.newArrayList("itemName"), restrTree.getProperty(REP_ITEM_NAMES).getValue(Type.NAMES));
    }
}