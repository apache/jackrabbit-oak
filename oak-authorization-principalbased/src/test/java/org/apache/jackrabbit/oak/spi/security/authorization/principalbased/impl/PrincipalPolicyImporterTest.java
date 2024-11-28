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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.NodeInfo;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.apache.jackrabbit.oak.spi.xml.TextValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_ITEM_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NODE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_ENTRY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_NAME;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.MockUtility.mockTree;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrincipalPolicyImporterTest extends AbstractPrincipalBasedTest {

    private FilterProvider filterProvider;
    private PrincipalPolicyImporter importer;

    @Override
    public void before() throws Exception {
        super.before();
        filterProvider = spy(getFilterProvider());
        importer = new PrincipalPolicyImporter(filterProvider, getMgrProvider(root));
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters params = ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT);
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, params);
    }

    private boolean  init(boolean isWorkspaceImport, int uuidBehavior) {
        return importer.init(mock(Session.class), root, getNamePathMapper(), isWorkspaceImport, uuidBehavior, new ReferenceChangeTracker(), getSecurityProvider());
    }

    private PropInfo mockPropInfo(@NotNull String jcrName) {
        return when(mock(PropInfo.class).getName()).thenReturn(jcrName).getMock();
    }

    private PropInfo mockPropInfo(@NotNull Principal principal) throws RepositoryException {
        TextValue tx = when(mock(TextValue.class).getString()).thenReturn(principal.getName()).getMock();
        PropInfo propInfo = mockPropInfo(getJcrName(REP_PRINCIPAL_NAME));
        when(propInfo.getTextValue()).thenReturn(tx);
        return propInfo;
    }

    private List<PropInfo> mockPropInfos(@Nullable String effectivePath,  @NotNull String... privNames) throws RepositoryException {
        List<PropInfo> propInfos = new ArrayList();
        if (effectivePath != null) {
            TextValue tx = when(mock(TextValue.class).getString()).thenReturn(effectivePath).getMock();
            PropInfo propInfo = mockPropInfo(getJcrName(REP_EFFECTIVE_PATH));
            when(propInfo.getTextValue()).thenReturn(tx);
            when(propInfo.getType()).thenReturn(PropertyType.PATH);
            propInfos.add(propInfo);
        }
        List privTxtValues = new ArrayList();
        for (String privName : privNames) {
            TextValue tx = when(mock(TextValue.class).getString()).thenReturn(getJcrName(privName)).getMock();
            privTxtValues.add(tx);
        }
        if (!privTxtValues.isEmpty()) {
            PropInfo propInfo = mockPropInfo(getJcrName(REP_PRIVILEGES));
            when(propInfo.getTextValues()).thenReturn(privTxtValues);
            when(propInfo.getType()).thenReturn(PropertyType.NAME);
            propInfos.add(propInfo);
        }
        return propInfos;
    }

    private List<PropInfo> mockPropInfos(@NotNull Map<String, String> restrictions, int propertyType) throws RepositoryException {
        return mockPropInfos(Maps.transformValues(restrictions, string -> {
            try {
                return new Value[] {getValueFactory(root).createValue(string, propertyType)};
            } catch (ValueFormatException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    private List<PropInfo> mockPropInfos(@NotNull Map<String, Value[]> restrictions) throws RepositoryException {
        List<PropInfo> propInfos = new ArrayList();
        for (Map.Entry<String,Value[]> r : restrictions.entrySet()) {
            String jcrName = r.getKey();
            List<Value> vs = ImmutableList.copyOf(r.getValue());
            PropInfo propInfo = mockPropInfo(jcrName);
            if (!vs.isEmpty()) {
                TextValue first = when(mock(TextValue.class).getString()).thenReturn(vs.get(0).getString()).getMock();
                when(propInfo.getTextValue()).thenReturn(first);
                when(propInfo.getValues(anyInt())).thenReturn(vs);
            }
            propInfos.add(propInfo);
        }
        return propInfos;
    }

    private PropertyDefinition mockPropertyDefinition(@NotNull String jcrName) {
        NodeType nt = when(mock(NodeType.class).getName()).thenReturn(jcrName).getMock();
        return when(mock(PropertyDefinition.class).getDeclaringNodeType()).thenReturn(nt).getMock();
    }

    private NodeInfo mockNodeInfo(@NotNull String jcrName, @NotNull String jcrPrimaryType) {
        NodeInfo ni = when(mock(NodeInfo.class).getName()).thenReturn(jcrName).getMock();
        when(ni.getPrimaryTypeName()).thenReturn(jcrPrimaryType);
        return ni;
    }

    private Tree createPolicyTree(@NotNull User user) throws RepositoryException {
        Tree t = root.getTree(getNamePathMapper().getOakPath(user.getPath()));
        return TreeUtil.getOrAddChild(t, REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY);
    }

    private String getJcrName(@NotNull String oakName) {
        return getNamePathMapper().getJcrName(oakName);
    }

    @Nullable
    private static PrincipalPolicyImpl.EntryImpl assertPolicy(@NotNull AccessControlPolicy[] policies, int expectedEntries) {
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof PrincipalPolicyImpl);
        assertEquals(expectedEntries, (((PrincipalPolicyImpl) policies[0])).size());
        if (expectedEntries > 0) {
            return ((PrincipalPolicyImpl) policies[0]).getEntries().get(0);
        } else {
            return null;
        }
    }

    @Test
    public void testInitWorkspaceImport() {
        assertTrue(init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW));
    }

    @Test
    public void testInitSessionImport() {
        assertTrue(init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING));
    }

    @Test(expected = IllegalStateException.class)
    public void testInitTwice() {
        assertTrue(init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW));
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);
    }

    @Test
    public void testInitResetMgrProviderRoot() {
        MgrProvider mp = when(mock(MgrProvider.class).getSecurityProvider()).thenReturn(getSecurityProvider()).getMock();
        importer = new PrincipalPolicyImporter(filterProvider, mp);
        init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        verify(mp, times(1)).reset(root, getNamePathMapper());
    }

    @Test
    public void testInitGetsFilter() {
        init(false,ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        verify(filterProvider, times(1)).getFilter(getSecurityProvider(), root, getNamePathMapper());
    }

    @Test
    public void testProcessReferencesIsNop() {
        // no initialization required
        importer.processReferences();

        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        importer.processReferences();
    }

    @Test(expected = IllegalStateException.class)
    public void testHandlePropInfoNotInitialized() throws Exception {
        importer.handlePropInfo(mock(Tree.class), mock(PropInfo.class), mock(PropertyDefinition.class));
    }

    @Test
    public void testHandlePropInfoNonExistingTree() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        Tree tree = when(mock(Tree.class).exists()).thenReturn(false).getMock();
        assertFalse(importer.handlePropInfo(tree, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoWrongTreeName() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

        // wrong policy name
        Tree tree = mockTree(AccessControlConstants.REP_POLICY, NT_REP_PRINCIPAL_POLICY, true);
        assertFalse(importer.handlePropInfo(tree, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoWrongTreeNt() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

        // wrong nt name
        Tree tree = mockTree(REP_PRINCIPAL_POLICY, AccessControlConstants.NT_REP_ACL, true);
        assertFalse(importer.handlePropInfo(tree, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoInvalidPropInfoName() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true);
        assertFalse(importer.handlePropInfo(tree, mockPropInfo("wrongName"), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoNullPropInfoName() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true);
        assertFalse(importer.handlePropInfo(tree, mock(PropInfo.class), mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoOakPropInfoName() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true);
        PropInfo propInfo = mockPropInfo(REP_PRINCIPAL_NAME);
        
        assertFalse(importer.handlePropInfo(tree, propInfo, mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandlePropInfoDefinitionMultiple() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true);
        PropInfo propInfo = mockPropInfo(getJcrName(REP_PRINCIPAL_NAME));
        PropertyDefinition def = when(mock(PropertyDefinition.class).isMultiple()).thenReturn(true).getMock();

        assertFalse(importer.handlePropInfo(tree, propInfo, def));
    }

    @Test
    public void testHandlePropInfoInvalidDeclaringNtName() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true);
        PropInfo propInfo = mockPropInfo(getJcrName(REP_PRINCIPAL_NAME));
        PropertyDefinition def = mockPropertyDefinition("wrongDeclaringNtName");

        assertFalse(importer.handlePropInfo(tree, propInfo, def));
    }

    @Test
    public void testHandlePropInfoOakDeclaringNtName() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, true);
        PropInfo propInfo = mockPropInfo(getJcrName(REP_PRINCIPAL_NAME));
        PropertyDefinition def = mockPropertyDefinition(NT_REP_PRINCIPAL_POLICY);

        assertFalse(importer.handlePropInfo(tree, propInfo, def));
    }

    @Test
    public void testHandlePropInfoUnsupportedPath() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, "/some/unsupported/path");
        PropInfo propInfo = mockPropInfo(getJcrName(REP_PRINCIPAL_NAME));
        PropertyDefinition def = mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY));

        assertFalse(importer.handlePropInfo(tree, propInfo, def));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testHandlePropInfoPrincipalNameMismatch() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);

        importer.handlePropInfo(createPolicyTree(getTestSystemUser()), mockPropInfo(new PrincipalImpl("mismatch")), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));
    }

    @Test
    public void testHandlePropInfoPrincipalNotHandled() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        User testUser = getTestUser();
        String wrongUserPath = PathUtils.concat(SUPPORTED_PATH , "testUser");

        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, PathUtils.concat(wrongUserPath, REP_PRINCIPAL_POLICY));
        assertFalse(importer.handlePropInfo(tree, mockPropInfo(testUser.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY))));
    }

    @Test
    public void testHandlePropInfoPrincipalByNameReturnsNull() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

        String oakPath = PathUtils.concat(SUPPORTED_PATH, "unknownSystemUser", REP_PRINCIPAL_POLICY);
        Tree tree = mockTree(REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY, oakPath);
        assertFalse(importer.handlePropInfo(tree, mockPropInfo(new PrincipalImpl("notFound")), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY))));
    }

    @Test
    public void testHandlePropInfoValidPrincipal() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        User user = getTestSystemUser();
        assertTrue(importer.handlePropInfo(createPolicyTree(user), mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY))));
    }

    @Test(expected = IllegalStateException.class)
    public void testPropertiesCompletedNotInitialized() throws Exception  {
        importer.propertiesCompleted(mock(Tree.class));
    }

    @Test
    public void testPropertiesCompletedNoPolicy() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        importer.propertiesCompleted(mock(Tree.class));
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testPropertiesCompletedWrongTree() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);

        User user = getTestSystemUser();
        importer.handlePropInfo(createPolicyTree(user), mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.propertiesCompleted(mockTree("/another/path", true));
        assertEquals(0, getAccessControlManager(root).getPolicies(user.getPrincipal()).length);
        assertFalse(root.getTree(getNamePathMapper().getOakPath(user.getPath())).hasChild(REP_PRINCIPAL_POLICY));
    }

    @Test
    public void testPropertiesCompletedSetsPolicy() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        String oakPath = policyTree.getPath();
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.propertiesCompleted(policyTree);

        assertTrue(root.getTree(oakPath).hasProperty(REP_PRINCIPAL_NAME));
        assertPolicy(getAccessControlManager(root).getPolicies(user.getPrincipal()), 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testStartNotInitialized() {
        importer.start(mock(Tree.class));
    }

    @Test
    public void testStartWithoutPolicy() {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        assertFalse(importer.start(mock(Tree.class)));
    }


    @Test
    public void testStartWithPolicyAndValidTree() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));
        importer.propertiesCompleted(policyTree);

        assertTrue(importer.start(policyTree));
    }

    @Test
    public void testStartInvalidTree() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));
        importer.propertiesCompleted(policyTree);

        assertFalse(importer.start(mockTree("/another/path", true)));
    }

    @Test(expected = IllegalStateException.class)
    public void testEndWithoutPolicy() throws Exception {
        importer.end(mock(Tree.class));
    }

    @Test
    public void testEndWithInvalidTree() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));
        importer.propertiesCompleted(policyTree);

        importer.end(mockTree("/another/path", true));

        // end-call must have removed the policy due to the mismatch
        assertFalse(root.getTree(policyTree.getPath()).exists());
    }

    @Test
    public void testEndWithPolicyAndValidTree() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));
        importer.propertiesCompleted(policyTree);

        importer.end(policyTree);

        policyTree = root.getTree(policyTree.getPath());
        assertTrue(policyTree.exists());
        assertTrue(policyTree.hasProperty(REP_PRINCIPAL_NAME));
        assertTrue(Iterables.isEmpty(policyTree.getChildren()));
    }

    @Test(expected = IllegalStateException.class)
    public void testStartChildInfoWithoutPolicy() throws Exception {
        importer.startChildInfo(mock(NodeInfo.class), mock(List.class));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoInvalidPrimaryType() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("invalidACE", getJcrName(AccessControlConstants.NT_REP_GRANT_ACE)), mock(List.class));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoOakPrimaryType() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("oakName", NT_REP_PRINCIPAL_ENTRY), mock(List.class));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoNestedEntry() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/anyPath", PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        importer.startChildInfo(mockNodeInfo("anotherEntry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/anyPath", PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoMissingPrivileges() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/effective/path"));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoRestrictionNotNestedInEntry() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mock(List.class));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoUnsupportedProperty() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        List<PropInfo> propInfos = mockPropInfos("/effective/path", PrivilegeConstants.JCR_REMOVE_CHILD_NODES);
        propInfos.add(mockPropInfo("unsupportedProperty"));
        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), propInfos);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoWrongEffectivePathPropertyType() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        List<PropInfo> propInfos = mockPropInfos(null, PrivilegeConstants.JCR_REMOVE_CHILD_NODES);
        // effective path with wrong type
        TextValue tx = when(mock(TextValue.class).getString()).thenReturn("/effective/path").getMock();
        PropInfo propInfo = mockPropInfo(getJcrName(REP_EFFECTIVE_PATH));
        when(propInfo.getTextValue()).thenReturn(tx);
        when(propInfo.getType()).thenReturn(PropertyType.STRING);
        propInfos.add(propInfo);

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), propInfos);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testStartChildInfoWrongPrivilegesPropertyType() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        List<PropInfo> propInfos = mockPropInfos("/effective/path");
        // rep:privileges with wrong type
        PropInfo propInfo = mockPropInfo(getJcrName(REP_PRIVILEGES));
        TextValue tx = when(mock(TextValue.class).getString()).thenReturn(getJcrName(JCR_READ)).getMock();
        List values = ImmutableList.of(tx);
        when(propInfo.getTextValues()).thenReturn(values);
        when(propInfo.getType()).thenReturn(PropertyType.STRING);
        propInfos.add(propInfo);

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), propInfos);
    }

    @Test(expected = IllegalStateException.class)
    public void testStartChildInfoNestedRestriction() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/effective/path", PrivilegeConstants.REP_WRITE));
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of(getJcrName(REP_GLOB), "/some/glob"), PropertyType.STRING));
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of()));
    }

    @Test(expected = IllegalStateException.class)
    public void testStartChildInfoNestedMvRestriction() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/effective/path", PrivilegeConstants.REP_WRITE));
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of(getJcrName(REP_NT_NAMES), new Value[0])));
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of()));
    }

    @Test(expected = IllegalStateException.class)
    public void testStartChildInfoRepNodeRestrictionOverwritesEffectivePath() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/effective/path", PrivilegeConstants.REP_READ_PROPERTIES));
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of(
                getJcrName(REP_NODE_PATH), "/effective/path"), PropertyType.STRING));
    }

    @Test
    public void testStartChildInfoRepNodeRestriction() throws Exception {
        init(true, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.start(policyTree);
        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos(null, PrivilegeConstants.REP_READ_PROPERTIES));
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of(
                getJcrName(REP_NODE_PATH), "/effective/path"), PropertyType.STRING));

        importer.endChildInfo();
        importer.endChildInfo();
        importer.end(policyTree);

        PrincipalPolicyImpl.EntryImpl entry = assertPolicy(getAccessControlManager(root).getPolicies(user.getPrincipal()), 1);
        assertEquals("/effective/path", entry.getEffectivePath());
    }

    @Test
    public void testStartChildInfoMvRestrictions() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.start(policyTree);
        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/effective/path", PrivilegeConstants.REP_WRITE));

        ValueFactory vf = getValueFactory(root);
        Value[] nameValues = new Value[] {vf.createValue(getJcrName("jcr:content"), PropertyType.NAME), vf.createValue(getJcrName("jcr:data"), PropertyType.NAME)};
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of(
                getJcrName(REP_ITEM_NAMES), nameValues,
                getJcrName(REP_GLOB), new Value[] {vf.createValue("/some/*/globbing")})
        ));
        importer.endChildInfo();
        importer.endChildInfo();
        importer.end(policyTree);

        PrincipalPolicyImpl.EntryImpl entry = assertPolicy(getAccessControlManager(root).getPolicies(user.getPrincipal()), 1);
        assertNotNull(entry.getRestrictions(getJcrName(REP_ITEM_NAMES)));
        assertNotNull(entry.getRestriction(getJcrName(REP_GLOB)));
    }

    @Test(expected = AccessControlException.class)
    public void testStartChildInfoUnsupportedRestrictions() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.start(policyTree);
        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos("/effective/path", PrivilegeConstants.REP_WRITE));
        importer.startChildInfo(mockNodeInfo(getJcrName(REP_RESTRICTIONS), getJcrName(NT_REP_RESTRICTIONS)), mockPropInfos(Map.of(
                "unsupported", "x"), PropertyType.STRING));
        // adding entry to policy must already throw the exception
        importer.endChildInfo();
    }

    @Test(expected = IllegalStateException.class)
    public void testEndChildInfoWithoutPolicyNorEntry() throws Exception {
        importer.endChildInfo();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEndChildInfoMissingEffectivePath() throws Exception {
        init(false, ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW);
        User user = getTestSystemUser();
        Tree policyTree = createPolicyTree(user);
        importer.handlePropInfo(policyTree, mockPropInfo(user.getPrincipal()), mockPropertyDefinition(getJcrName(NT_REP_PRINCIPAL_POLICY)));

        importer.startChildInfo(mockNodeInfo("entry", getJcrName(NT_REP_PRINCIPAL_ENTRY)), mockPropInfos(null, JCR_READ));
        importer.endChildInfo();
    }
}