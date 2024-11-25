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
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_ITEM_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.MIX_REP_PRINCIPAL_BASED_MIXIN;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_ENTRY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_NAME;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.MockUtility.createMixinTypesProperty;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.MockUtility.mockNodeState;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PolicyValidatorTest extends AbstractPrincipalBasedTest {

    private final NodeState mockNodeState = mock(NodeState.class);

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    @NotNull
    private Validator createRootValidator(@NotNull NodeState rootState) {
        return createRootValidator(rootState, getMgrProvider(root));
    }

    @NotNull
    private Validator createRootValidator(@NotNull NodeState rootState, @NotNull MgrProvider mgrProvider) {
        return new PrincipalPolicyValidatorProvider(mgrProvider, adminSession.getAuthInfo().getPrincipals(), adminSession.getWorkspaceName()).getRootValidator(rootState, rootState, new CommitInfo("anyId", null));
    }

    @NotNull
    private Validator getValidatorAtNodeTypeTree(@NotNull NodeState nodeState, @NotNull String parentName, boolean isAdd) throws Exception {
        Validator v = createRootValidator(nodeState);
        when(nodeState.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(createPrimaryTypeProperty(JcrConstants.NT_BASE));
        if (isAdd) {
            return v.childNodeAdded(parentName, nodeState).childNodeAdded(NodeTypeConstants.JCR_NODE_TYPES, nodeState);
        }  else {
            return v.childNodeChanged(parentName, nodeState, nodeState).childNodeChanged(NodeTypeConstants.JCR_NODE_TYPES, nodeState, nodeState);
        }
    }

    @NotNull
    private Tree createPolicyEntryTree(@NotNull Set<String> privNames) throws Exception {
        Tree t = root.getTree(getNamePathMapper().getOakPath(getTestSystemUser().getPath()));
        TreeUtil.addMixin(t, MIX_REP_PRINCIPAL_BASED_MIXIN, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
        Tree policy = TreeUtil.addChild(t, REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY);
        policy.setProperty(REP_PRINCIPAL_NAME, getTestSystemUser().getPrincipal().getName());
        Tree entry = TreeUtil.addChild(policy, "entry", NT_REP_PRINCIPAL_ENTRY);
        entry.setProperty(REP_EFFECTIVE_PATH, TEST_OAK_PATH, Type.PATH);
        entry.setProperty(REP_PRIVILEGES, privNames, Type.NAMES);
        return entry;
    }

    @NotNull
    private MgrProvider mockMgrProvider() {
        MgrProvider mp = mock(MgrProvider.class);
        when(mp.getRootProvider()).thenReturn(getRootProvider());
        when(mp.getTreeProvider()).thenReturn(getTreeProvider());
        when(mp.getSecurityProvider()).thenReturn(getSecurityProvider());
        return mp;
    }

    @NotNull
    private static PropertyState createPrimaryTypeProperty(@NotNull String ntName) {
        return MockUtility.createPrimaryTypeProperty(ntName);
    }

    private static void assertCommitFailed(@NotNull CommitFailedException e, int expectedErrorCode) {
        assertTrue(e.isAccessControlViolation());
        assertEquals(expectedErrorCode, e.getCode());
    }

    private static void failCommitFailedExpected(int expectedCode) {
        fail("Expected CommitFailedException with ErrorCode " +expectedCode);
    }

    @Test
    public void testArbitraryPropertyAdded() throws Exception {
        createRootValidator(mockNodeState).propertyAdded(PropertyStates.createProperty("any", NT_REP_PRINCIPAL_POLICY));
    }

    @Test
    public void testPrimaryTypePropertyAdded() throws Exception {
        createRootValidator(mockNodeState).propertyAdded(createPrimaryTypeProperty(JcrConstants.NT_UNSTRUCTURED));
    }

    @Test
    public void testPolicyPrimaryTypePropertyAddedWrongParentName() throws Exception {
        Validator v = createRootValidator(mockNodeState).childNodeAdded("wrongName", mockNodeState);
        try {
            v.propertyAdded(createPrimaryTypeProperty(NT_REP_PRINCIPAL_POLICY));
            failCommitFailedExpected(30);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 30);
        }
    }

    @Test
    public void testPolicyPrimaryTypePropertyAdded() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        TreeUtil.addMixin(rootTree, MIX_REP_PRINCIPAL_BASED_MIXIN, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");

        NodeState rootState = getTreeProvider().asNodeState(rootTree);
        NodeState child = mockNodeState(NT_REP_PRINCIPAL_POLICY);
        Validator v = createRootValidator(rootState).childNodeAdded(REP_PRINCIPAL_POLICY, child);
        v.propertyAdded(createPrimaryTypeProperty(NT_REP_PRINCIPAL_POLICY));
    }

    @Test
    public void testArbitraryPropertyChanged() throws Exception {
        PropertyState before = PropertyStates.createProperty("any", NT_REP_PRINCIPAL_POLICY);
        PropertyState after = PropertyStates.createProperty("any", NT_REP_PRINCIPAL_ENTRY);
        createRootValidator(mockNodeState).propertyChanged(before, after);
    }

    @Test
    public void testPrimaryTypePropertyChanged() throws Exception {
        PropertyState before = createPrimaryTypeProperty(JcrConstants.NT_UNSTRUCTURED);
        PropertyState after = createPrimaryTypeProperty(NT_OAK_UNSTRUCTURED);
        createRootValidator(mockNodeState).propertyChanged(before, after);
    }

    @Test
    public void testPolicyBeforePrimaryTypePropertyChanged() throws Exception {
        try {
            PropertyState before = createPrimaryTypeProperty(NT_REP_PRINCIPAL_POLICY);
            PropertyState after = createPrimaryTypeProperty(NT_OAK_UNSTRUCTURED);
            createRootValidator(mockNodeState).propertyChanged(before, after);
            failCommitFailedExpected(31);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 31);
        }
    }

    @Test
    public void testPolicyAfterPrimaryTypePropertyChanged() throws Exception {
        try {
            PropertyState before = createPrimaryTypeProperty(NT_OAK_UNSTRUCTURED);
            PropertyState after = createPrimaryTypeProperty(NT_REP_PRINCIPAL_POLICY);
            createRootValidator(mockNodeState).propertyChanged(before, after);
            failCommitFailedExpected(31);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 31);
        }
    }

    @Test
    public void testArbitraryPropertyDeleted() throws Exception {
        createRootValidator(mockNodeState).propertyDeleted(PropertyStates.createProperty("any", NT_REP_PRINCIPAL_POLICY));
    }

    @Test
    public void testPrimaryTypePropertyDeleted() throws Exception {
        createRootValidator(mockNodeState).propertyDeleted(createPrimaryTypeProperty(JcrConstants.NT_UNSTRUCTURED));
    }

    @Test
    public void testAccessControlPrimaryTypePropertyDeleted() throws Exception {
        createRootValidator(mockNodeState).propertyDeleted(createPrimaryTypeProperty(NT_REP_PRINCIPAL_POLICY));
        createRootValidator(mockNodeState).propertyDeleted(createPrimaryTypeProperty(NT_REP_RESTRICTIONS));
        createRootValidator(mockNodeState).propertyDeleted(createPrimaryTypeProperty(NT_REP_PRINCIPAL_ENTRY));
    }

    @Test
    public void testChildAddedToNodeTypeTree() throws Exception {
        Validator validator = getValidatorAtNodeTypeTree(mockNodeState, JCR_SYSTEM, true);
        validator.childNodeAdded(REP_PRINCIPAL_POLICY, mockNodeState);
    }

    @Test
    public void testChildChangedToNodeTypeTree() throws Exception {
        Validator validator = getValidatorAtNodeTypeTree(mockNodeState, JCR_SYSTEM, false);
        validator.childNodeChanged(REP_PRINCIPAL_POLICY, mockNodeState, mockNodeState);
    }

    @Test
    public void testChildChangedBelowNodeTypeTree() throws Exception {
        Validator validator = getValidatorAtNodeTypeTree(mockNodeState, JCR_SYSTEM, false);
        validator.childNodeChanged("any", mockNodeState, mockNodeState).childNodeChanged(REP_PRINCIPAL_POLICY, mockNodeState, mockNodeState);
    }

    @Test
    public void testChildDeletedToNodeTypeTree() throws Exception {
        Validator validator = getValidatorAtNodeTypeTree(mockNodeState, JCR_SYSTEM, false);

        when(mockNodeState.hasChildNode(REP_PRINCIPAL_POLICY)).thenReturn(true);
        validator.childNodeDeleted(REP_PRINCIPAL_POLICY, mockNodeState);
    }

    @Test
    public void tetPolicyChildAddedWrongType() {
        NodeState child = mockNodeState(NodeTypeConstants.NT_REP_UNSTRUCTURED);
        try {
            createRootValidator(mockNodeState).childNodeAdded(REP_PRINCIPAL_POLICY, child);
            failCommitFailedExpected(32);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 32);
        }
    }

    @Test
    public void tetPolicyChildAddedMissingMixinOnParent() {
        NodeState rootState = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        assertFalse(Iterables.contains(rootState.getNames(JcrConstants.JCR_MIXINTYPES), MIX_REP_PRINCIPAL_BASED_MIXIN));

        NodeState child = mockNodeState(NT_REP_PRINCIPAL_POLICY);
        try {
            createRootValidator(rootState).childNodeAdded(REP_PRINCIPAL_POLICY, child);
            failCommitFailedExpected(33);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 33);
        }
    }

    @Test
    public void testPolicyChildAdded() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        TreeUtil.addMixin(rootTree, MIX_REP_PRINCIPAL_BASED_MIXIN, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");

        NodeState child = mockNodeState(NT_REP_PRINCIPAL_POLICY);
        Validator v = createRootValidator(getTreeProvider().asNodeState(rootTree)).childNodeAdded(REP_PRINCIPAL_POLICY, child);
        assertTrue(v instanceof VisibleValidator);
    }

    @Test
    public void tetChildChangedWrongType() {
        NodeState ns = mockNodeState(NT_OAK_UNSTRUCTURED);
        NodeState child = mockNodeState(NodeTypeConstants.NT_REP_UNSTRUCTURED);
        when(child.hasChildNode(REP_PRINCIPAL_POLICY)).thenReturn(true);
        when(child.getChildNode(REP_PRINCIPAL_POLICY)).thenReturn(ns);
        try {
            createRootValidator(mockNodeState).childNodeChanged(REP_PRINCIPAL_POLICY, child, child);
            failCommitFailedExpected(32);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 32);
        }
    }

    @Test
    public void tetChildChangedMissingMixin() {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));

        when(mockNodeState.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(createPrimaryTypeProperty(NT_REP_PRINCIPAL_POLICY));

        NodeState child = mockNodeState(NT_OAK_UNSTRUCTURED);
        when(child.hasChildNode(REP_PRINCIPAL_POLICY)).thenReturn(true);
        when(child.getChildNode(REP_PRINCIPAL_POLICY)).thenReturn(mockNodeState);
        try {
            createRootValidator(rootState).childNodeChanged("any", child, child);
            failCommitFailedExpected(33);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 33);
        }
    }

    @Test
    public void testArbitraryNodeTypeTreeTriggersValidation() throws Exception {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));
        Validator validator = getValidatorAtNodeTypeTree(rootState, "notJcrSystem", true);

        NodeState child = mockNodeState(NT_REP_PRINCIPAL_POLICY);
        try {
            validator.childNodeAdded(REP_PRINCIPAL_POLICY, child);
            failCommitFailedExpected(33);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 33);
        }
    }

    @Test
    public void testArbitraryNodeTypeTreeTriggersValidation2() throws Exception {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));
        Validator validator = getValidatorAtNodeTypeTree(rootState, "notJcrSystem", false);

        when(mockNodeState.hasChildNode(REP_PRINCIPAL_POLICY)).thenReturn(true);
        when(mockNodeState.getChildNode(REP_PRINCIPAL_POLICY)).thenReturn(mockNodeState);
        try {
            validator.childNodeChanged(REP_PRINCIPAL_POLICY, mockNodeState, mockNodeState);
            failCommitFailedExpected(32);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 32);
        }
    }

    @Test
    public void testAddRestrictionWithWrongNtName() {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));
        NodeState restrictions = mockNodeState(NT_OAK_UNSTRUCTURED);
        try {
            Validator v = createRootValidator(rootState).childNodeAdded(REP_RESTRICTIONS, restrictions);
            failCommitFailedExpected(34);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 34);
        }
    }

    @Test
    public void testAddIsolatedRestriction() {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));

        NodeState restrictions = mockNodeState(NT_REP_RESTRICTIONS);
        try {
            createRootValidator(rootState).childNodeAdded(REP_RESTRICTIONS, restrictions);
            failCommitFailedExpected(2);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 2);
        }
    }

    @Test
    public void testAddUnknownRestriction() throws Exception {
        Tree entry = createPolicyEntryTree(Set.of(JCR_READ));
        Tree restrictions = TreeUtil.addChild(entry, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setProperty("unknown", "test");

        try {
            root.commit();
            failCommitFailedExpected(35);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 35);
        }
    }

    @Test
    public void testAddInvalidRestriction() throws Exception {
        Tree entry = createPolicyEntryTree(Set.of(JCR_READ));
        Tree restrictions = TreeUtil.addChild(entry, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        // wrong type. must be NAMES.
        restrictions.setProperty(REP_ITEM_NAMES, Set.of("test"), Type.STRINGS);
        try {
            root.commit();
            failCommitFailedExpected(35);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 35);
        }
    }

    @Test
    public void testAddRestrictionWithDifferentACE() throws Exception {
        // validator must not complain about adding restrictions to a different authorization model
        Map<String, Value> restr = Map.of(getNamePathMapper().getJcrName(REP_GLOB), getValueFactory(root).createValue("val"));
        addDefaultEntry(PathUtils.ROOT_PATH, EveryonePrincipal.getInstance(), restr, null, JCR_LIFECYCLE_MANAGEMENT);
        root.commit();
    }

    @Test
    public void testChangeWithInvalidRestriction() throws Exception {
        Tree entry = createPolicyEntryTree(Set.of(JCR_READ));
        Tree restrictions = TreeUtil.addChild(entry, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setProperty(REP_GLOB, "*/glob/*");
        root.commit();

        // modify restriction tree changing glob property with type-cardinality mismatch
        restrictions.setProperty(REP_GLOB, Set.of("test"), Type.STRINGS);
        try {
            root.commit();
            failCommitFailedExpected(35);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 35);
        }
    }

    @Test
    public void testChangeFromSingleValuedToMvRestriction() throws Exception {
        Tree entry = createPolicyEntryTree(Set.of(JCR_READ));
        Tree restrictions = TreeUtil.addChild(entry, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setProperty(REP_GLOB, "*/glob/*");
        root.commit();

        restrictions.removeProperty(REP_GLOB);
        restrictions.setProperty(REP_ITEM_NAMES, Set.of("someName", "anotherName"), Type.NAMES);
        root.commit();
    }

    @Test
    public void testChangeWithThrowingRestrictionValidation() throws Exception {
        NodeState entryMock = mockNodeState(NT_REP_PRINCIPAL_ENTRY);
        when(entryMock.getProperty(REP_EFFECTIVE_PATH)).thenReturn(PropertyStates.createProperty(REP_EFFECTIVE_PATH, "/path"));
        NodeState restrictions = mockNodeState(NT_REP_RESTRICTIONS);

        RestrictionProvider throwingRp = mock(RestrictionProvider.class);
        doThrow(new RepositoryException()).when(throwingRp).validateRestrictions(anyString(), any(Tree.class));

        MgrProvider mgrProvider = mockMgrProvider();
        when(mgrProvider.getContext()).thenReturn(getConfig(AuthorizationConfiguration.class).getContext());
        when(mgrProvider.getRestrictionProvider()).thenReturn(throwingRp);

        try {
            createRootValidator(entryMock, mgrProvider).childNodeChanged(REP_RESTRICTIONS, restrictions, restrictions);
            failCommitFailedExpected(13);
        } catch (CommitFailedException e) {
            assertTrue(e.isOfType(CommitFailedException.OAK));
            assertEquals(13, e.getCode());        }
    }

    @Test
    public void testAddIsolatedEntry() throws Exception {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));

        try {
            Validator v = createRootValidator(rootState).childNodeAdded("anyName", mockNodeState(NT_REP_PRINCIPAL_ENTRY));
            failCommitFailedExpected(36);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 36);
        }
    }

    @Test
    public void testAddEntryWithEmptyPrivilegeSet() throws Exception {
        Tree entry = createPolicyEntryTree(Set.of());
        try {
            root.commit();
            failCommitFailedExpected(37);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 37);
        }
    }

    @Test
    public void testAddEntryWithAbstractPrivilege() throws Exception {
        getPrivilegeManager(root).registerPrivilege("abstractPriv", true, new String[0]);

        Tree entry = createPolicyEntryTree(Set.of("abstractPriv"));
        try {
            root.commit();
            failCommitFailedExpected(38);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 38);
        }
    }

    @Test
    public void testAddEntryWithInvalidPrivilege() throws Exception {
        Tree entry = createPolicyEntryTree(Set.of("invalidPrivilege"));
        try {
            root.commit();
            failCommitFailedExpected(39);
        } catch (CommitFailedException e) {
            assertCommitFailed(e, 39);
        }
    }

    @Test
    public void testAddEntryPrivilegeLookupThrowsRepositoryException() throws Exception {
        PrivilegeManager privMgr = when(mock(PrivilegeManager.class).getPrivilege(anyString())).thenThrow(new RepositoryException()).getMock();
        MgrProvider mp = when(mockMgrProvider().getPrivilegeManager()).thenReturn(privMgr).getMock();

        NodeState policyState =  mockNodeState(NT_REP_PRINCIPAL_POLICY);
        when(policyState.getProperty(REP_PRINCIPAL_NAME)).thenReturn(PropertyStates.createProperty(REP_PRINCIPAL_NAME, "name"));

        NodeState ns = mockNodeState(NT_OAK_UNSTRUCTURED);
        when(ns.hasChildNode(REP_PRINCIPAL_POLICY)).thenReturn(true);
        when(ns.getChildNode(REP_PRINCIPAL_POLICY)).thenReturn(policyState);
        when(ns.getProperty(JCR_MIXINTYPES)).thenReturn(createMixinTypesProperty(MIX_REP_PRINCIPAL_BASED_MIXIN));

        Validator v = createRootValidator(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)), mp);
        v = v.childNodeChanged("any", ns, ns);
        v = v.childNodeChanged(REP_PRINCIPAL_POLICY, policyState, policyState);

        try {
            NodeState entry = mockNodeState(NT_REP_PRINCIPAL_ENTRY);
            when(entry.getProperty(REP_EFFECTIVE_PATH)).thenReturn(PropertyStates.createProperty(REP_EFFECTIVE_PATH,"/path", Type.PATH));
            when(entry.getNames(REP_PRIVILEGES)).thenReturn(ImmutableList.of("privName"));

            v.childNodeChanged("entryName", entry, entry);
            fail("CommitFailedException type OAK code 13 expected.");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.OAK, e.getType());
            assertEquals(13, e.getCode());
        }
    }

    @Test
    public void testAddEntryMissingEffectivePath() throws Exception {
        MgrProvider mp = when(mockMgrProvider().getPrivilegeManager()).thenReturn(getPrivilegeManager(root)).getMock();

        NodeState policyState =  mockNodeState(NT_REP_PRINCIPAL_POLICY);
        when(policyState.getProperty(REP_PRINCIPAL_NAME)).thenReturn(PropertyStates.createProperty(REP_PRINCIPAL_NAME, "name"));

        NodeState ns = mockNodeState(NT_OAK_UNSTRUCTURED);
        when(ns.hasChildNode(REP_PRINCIPAL_POLICY)).thenReturn(true);
        when(ns.getChildNode(REP_PRINCIPAL_POLICY)).thenReturn(policyState);
        when(ns.getProperty(JCR_MIXINTYPES)).thenReturn(createMixinTypesProperty(MIX_REP_PRINCIPAL_BASED_MIXIN));

        Validator v = createRootValidator(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)), mp);
        v = v.childNodeChanged("any", ns, ns);
        v = v.childNodeChanged(REP_PRINCIPAL_POLICY, policyState, policyState);

        try {
            NodeState entry = mockNodeState(NT_REP_PRINCIPAL_ENTRY);
            when(entry.getProperty(REP_EFFECTIVE_PATH)).thenReturn(null);
            when(entry.getNames(REP_PRIVILEGES)).thenReturn(ImmutableList.of(JCR_READ));

            v.childNodeChanged("entryName", entry, entry);
            fail("CommitFailedException type CONSTRAINT code 21 expected.");
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.CONSTRAINT, e.getType());
            assertEquals(21, e.getCode());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalAddDelete() throws Exception {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));
        NodeState child = mockNodeState(NT_OAK_UNSTRUCTURED);

        Validator v = createRootValidator(rootState).childNodeAdded("added", child);
        v.childNodeDeleted("deleted", child);
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalDeleteAdd() throws Exception {
        NodeState rootState = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));
        NodeState child = mockNodeState(NT_OAK_UNSTRUCTURED);

        Validator v = createRootValidator(rootState).childNodeDeleted("deleted", child);
        v.childNodeAdded("added", child);
    }
}