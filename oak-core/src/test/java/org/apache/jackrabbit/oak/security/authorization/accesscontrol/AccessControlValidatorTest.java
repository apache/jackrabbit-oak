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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderHelper;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.AccessDeniedException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class AccessControlValidatorTest extends AbstractSecurityTest implements AccessControlConstants {

    private final String testName = "testRoot";
    private final String testPath = '/' + testName;
    private final String aceName = "validAce";

    private Principal testPrincipal;

    @Before
    public void before() throws Exception {
        super.before();

        TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), testName, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        testPrincipal = getTestUser().getPrincipal();
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Tree testRoot = root.getTree(testPath);
            if (testRoot.exists()) {
                testRoot.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @NotNull
    private Tree getTestTree() {
        return root.getTree(testPath);
    }

    @NotNull
    private AccessControlValidatorProvider createValidatorProvider() {
        CompositeAuthorizationConfiguration cac = (CompositeAuthorizationConfiguration) getConfig(AuthorizationConfiguration.class);
        return new AccessControlValidatorProvider((AuthorizationConfigurationImpl) cac.getDefaultConfig());
    }

    @NotNull
    private AccessControlValidatorProvider createValidatorProvider(@NotNull RestrictionProvider restrictionProvider, @NotNull PrivilegeConfiguration privilegeConfiguration) {
        ProviderCtx ctx = mock(ProviderCtx.class);
        when(ctx.getRootProvider()).thenReturn(getRootProvider());
        when(ctx.getTreeProvider()).thenReturn(getTreeProvider());

        SecurityProvider sp = mock(SecurityProvider.class);
        when(sp.getConfiguration(PrivilegeConfiguration.class)).thenReturn(privilegeConfiguration);
        AuthorizationConfiguration ac = mock(AuthorizationConfiguration.class);
        when(ac.getRestrictionProvider()).thenReturn(restrictionProvider);
        when(ac.getContext()).thenReturn(getConfig(AuthorizationConfiguration.class).getContext());
        when(sp.getConfiguration(AuthorizationConfiguration.class)).thenReturn(ac);
        when(ctx.getSecurityProvider()).thenReturn(sp);
        return new AccessControlValidatorProvider(ctx);
    }

    @NotNull
    private Validator createRootValidator(@NotNull Tree rootTree) {
        NodeState ns = getTreeProvider().asNodeState(rootTree);
        return createValidatorProvider().getRootValidator(ns, ns, new CommitInfo("sid", null));
    }

    @NotNull
    private Tree createPolicy(@NotNull Tree tree, boolean createRestrictionNode) throws AccessDeniedException {
        tree.setProperty(JCR_MIXINTYPES, ImmutableList.of(MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES);

        Tree acl = TreeUtil.addChild(tree, REP_POLICY, NT_REP_ACL);
        acl.setOrderableChildren(true);
        Tree ace = createACE(acl, aceName, NT_REP_GRANT_ACE, testPrincipal.getName(), JCR_READ);
        if (createRestrictionNode) {
            TreeUtil.addChild(ace, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        }
        return acl;
    }

    @NotNull
    private static Tree createACE(@NotNull Tree acl, @NotNull String aceName, @NotNull String ntName, @NotNull String principalName, @NotNull String... privilegeNames) throws AccessDeniedException {
        Tree ace = TreeUtil.addChild(acl, aceName, ntName);
        ace.setProperty(REP_PRINCIPAL_NAME, principalName);
        ace.setProperty(REP_PRIVILEGES, ImmutableList.copyOf(privilegeNames), Type.NAMES);
        return ace;
    }

    private static CommitFailedException assertCommitFailedException(@NotNull CommitFailedException e, @NotNull String type, int expectedCode) {
        assertTrue(e.isOfType(type));
        assertEquals(expectedCode, e.getCode());
        return e;
    }

    @Test(expected = CommitFailedException.class)
    public void testPolicyWithOutChildOrder() throws Exception {
        Tree testRoot = getTestTree();
        testRoot.setProperty(JCR_MIXINTYPES, ImmutableList.of(MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES);
        TreeUtil.addChild(testRoot, REP_POLICY, NT_REP_ACL);

        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 4);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testOnlyRootIsRepoAccessControllable() throws Exception {
        Tree testRoot = getTestTree();
        testRoot.setProperty(JCR_MIXINTYPES, ImmutableList.of(MIX_REP_REPO_ACCESS_CONTROLLABLE), Type.NAMES);

        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 12);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddInvalidRepoPolicy() throws Exception {
        Tree testRoot = getTestTree();
        testRoot.setProperty(JCR_MIXINTYPES, ImmutableList.of(MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES);
        TreeUtil.addChild(testRoot, REP_REPO_POLICY, NT_REP_ACL);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 6);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddPolicyWithAcl() throws Exception {
        addPolicyWithAcContent();
    }

    @Test(expected = CommitFailedException.class)
    public void testAddPolicyWithAce() throws Exception {
        addPolicyWithAcContent(aceName);
    }

    @Test(expected = CommitFailedException.class)
    public void testAddPolicyWithRestriction() throws Exception {
        addPolicyWithAcContent(aceName, REP_RESTRICTIONS);
    }

    private void addPolicyWithAcContent(@NotNull String... childNames) throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree policy = createPolicy(rootTree, true);
        Tree acContent = policy;
        for (String name : childNames) {
            acContent = acContent.getChild(name);
        }
        TreeUtil.addChild(acContent, REP_POLICY, NT_REP_ACL);

        TreeProvider tp = getTreeProvider();
        Validator v = createRootValidator(rootTree);
        try {
            v = v.childNodeAdded(policy.getName(), tp.asNodeState(policy));
            Tree t = policy;
            for (String name : childNames) {
                t = t.getChild(name);
                v = v.childNodeAdded(name, tp.asNodeState(t));
            }
            v.childNodeAdded(REP_POLICY, tp.asNodeState(t.getChild(REP_POLICY)));
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 5);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void tesAddIsolatedRepPolicy() throws Exception {
        addIsolatedPolicy(REP_POLICY);
    }

    @Test(expected = CommitFailedException.class)
    public void tesAddIsolatedRepRepoPolicy() throws Exception {
        addIsolatedPolicy(REP_REPO_POLICY);
    }

    @Test(expected = CommitFailedException.class)
    public void tesAddIsolatedUnknownPolicy() throws Exception {
        addIsolatedPolicy("isolatedPolicy");
    }

    private void addIsolatedPolicy(@NotNull String name) throws Exception {
        TreeUtil.addChild(getTestTree(), name, NT_REP_ACL);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 6);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddIsolatedGrantAce() throws Exception {
        createACE(getTestTree(), "isolatedACE", NT_REP_GRANT_ACE, testPrincipal.getName(), JCR_READ);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 7);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddIsolatedDenyAce() throws Exception {
        createACE(getTestTree(), "isolatedACE", NT_REP_DENY_ACE, testPrincipal.getName(), JCR_READ);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 7);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddIsolatedRestriction() throws Exception {
        TreeUtil.addChild(getTestTree(), "isolatedRestriction", NT_REP_RESTRICTIONS);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 2);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testInvalidPrivilege() throws Exception {
        createACE(createPolicy(getTestTree(), false), "aceWithInvalidPrivilege", NT_REP_GRANT_ACE, testPrincipal.getName(), "invalidPrivilegeName");
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 10);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAbstractPrivilege() throws Exception {
        PrivilegeManager pMgr = getPrivilegeManager(root);
        pMgr.registerPrivilege("abstractPrivilege", true, new String[0]);

        createACE(createPolicy(getTestTree(), false), "invalid", NT_REP_GRANT_ACE, testPrincipal.getName(), "abstractPrivilege");
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 11);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testValidatingPrivilegesCausesInternalError() throws Exception {
        PrivilegeManager privMgr = when(mock(PrivilegeManager.class).getPrivilege(anyString())).thenThrow(new RepositoryException()).getMock();
        PrivilegeConfiguration pc = when( mock(PrivilegeConfiguration.class).getPrivilegeManager(any(Root.class), any(NamePathMapper.class))).thenReturn(privMgr).getMock();

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree policy = createPolicy(rootTree, false);

        TreeProvider tp = getTreeProvider();
        AccessControlValidatorProvider provider = createValidatorProvider(getConfig(AuthorizationConfiguration.class).getRestrictionProvider(), pc);
        NodeState ns = tp.asNodeState(rootTree);
        Validator v = provider.getRootValidator(ns, ns, new CommitInfo("sid", null));

        v.childNodeAdded(policy.getName(), tp.asNodeState(policy)).childNodeAdded(aceName, tp.asNodeState(policy.getChild(aceName)));
    }

    @Test(expected = CommitFailedException.class)
    public void testInvalidRestriction() throws Exception {
        Tree restriction = createPolicy(getTestTree(), true).getChild(aceName).getChild(REP_RESTRICTIONS);
        restriction.setProperty("invalid", "value");
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 1);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRestrictionWithInvalidType() throws Exception {
        Tree restriction = createPolicy(getTestTree(), true).getChild(aceName).getChild(REP_RESTRICTIONS);
        restriction.setProperty(REP_GLOB, "rep:glob", Type.NAME);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 1);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testValidatingRestrictionsCausesInternalError() throws Exception {
        RestrictionProvider rp = spy(getConfig(AuthorizationConfiguration.class).getRestrictionProvider());
        doAnswer(invocationOnMock -> {
            throw new RepositoryException();
        }).when(rp).validateRestrictions(anyString(), any(Tree.class));

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree policy = createPolicy(rootTree, false);
        Tree entry = policy.getChild(aceName);
        entry.setProperty(REP_GLOB, "any");

        TreeProvider tp = getTreeProvider();
        AccessControlValidatorProvider provider = createValidatorProvider(rp, getConfig(PrivilegeConfiguration.class));
        NodeState ns = tp.asNodeState(rootTree);
        Validator v = provider.getRootValidator(ns, ns, new CommitInfo("sid", null));

        try {
            v.childNodeAdded(policy.getName(), tp.asNodeState(policy)).childNodeAdded(entry.getName(), tp.asNodeState(entry));
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.OAK, 13);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testDuplicateAce() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
        acMgr.setPolicy(testPath, acl);

        // add duplicate ac-entry on OAK-API
        createDuplicateAceTree();
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 13);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testDuplicateAceWithRestrictionInACE() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        ValueFactory vf = getValueFactory(root);
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES), true,
                Collections.singletonMap(REP_GLOB, vf.createValue("some/glob")));
        acMgr.setPolicy(testPath, acl);

        // add duplicate ac-entry on OAK-API with single and mv restriction
        Tree ace = createDuplicateAceTree();
        ace.setProperty(AccessControlConstants.REP_GLOB, "some/glob", Type.STRING);

        try {
            root.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.getMessage().contains("rep:glob = some/glob"));
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 13);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testDuplicateAceWithRestrictions() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        ValueFactory vf = getValueFactory(root);
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES), true, 
                Collections.singletonMap(REP_GLOB, vf.createValue("some/glob")),
                Collections.singletonMap(REP_GLOBS, new Value[] {vf.createValue("glob1"), vf.createValue("glob2")}));
        acMgr.setPolicy(testPath, acl);

        // add duplicate ac-entry on OAK-API with single and mv restriction
        Tree ace = createDuplicateAceTree();
        Tree rest = TreeUtil.addChild(ace, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        rest.setProperty(AccessControlConstants.REP_GLOB, "some/glob", Type.STRING);
        rest.setProperty(AccessControlConstants.REP_GLOBS, ImmutableList.of("glob1", "glob2"), Type.STRINGS);
        
        try {
            root.commit();
        } catch (CommitFailedException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains("rep:glob = some/glob"));
            assertTrue(msg.contains("rep:globs = [glob1, glob2]"));
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 13);
        }
    }
    
    private @NotNull Tree createDuplicateAceTree() throws AccessDeniedException {
        Tree policy = root.getTree(testPath + "/rep:policy");
        Tree ace = TreeUtil.addChild(policy, "duplicateAce", NT_REP_GRANT_ACE);
        ace.setProperty(REP_PRINCIPAL_NAME, testPrincipal.getName());
        ace.setProperty(AccessControlConstants.REP_PRIVILEGES, ImmutableList.of(PrivilegeConstants.JCR_ADD_CHILD_NODES), Type.NAMES);
        return ace;
    }

    @Test
    public void testAceDifferentByAllowStatus() throws Exception {
        Tree policy = createPolicy(root.getTree(PathUtils.ROOT_PATH), false);
        Tree entry = policy.getChild(aceName);
        Tree entry2 = TreeUtil.addChild(policy, "second", NT_REP_DENY_ACE);
        entry2.setProperty(entry.getProperty(REP_PRINCIPAL_NAME));
        entry2.setProperty(entry.getProperty(REP_PRIVILEGES));

        root.commit();
    }

    @Test
    public void testAceDifferentByRestrictionValue() throws Exception {
        ValueFactory vf = getValueFactory(root);

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES), true,
                Map.of(),
                Map.of(AccessControlConstants.REP_NT_NAMES, new Value[] {vf.createValue(NodeTypeConstants.NT_OAK_UNSTRUCTURED, PropertyType.NAME)}));

        // add ac-entry that only differs by the value of the restriction
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES), true,
                Map.of(),
                Map.of(AccessControlConstants.REP_NT_NAMES, new Value[] {vf.createValue(NodeTypeConstants.NT_UNSTRUCTURED, PropertyType.NAME)}));
        assertEquals(2, acl.getAccessControlEntries().length);

        acMgr.setPolicy(testPath, acl);

        // persisting changes must succeed; the 2 ac-entries must not be treated as equal.
        root.commit();
    }

    @Test
    public void hiddenNodeAdded() throws CommitFailedException {
        AccessControlValidatorProvider provider = createValidatorProvider();
        MemoryNodeStore store = new MemoryNodeStore();
        NodeState root = store.getRoot();
        NodeBuilder builder = root.builder();
        NodeBuilder test = builder.child("test");
        NodeBuilder hidden = test.child(":hidden");

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeAdded(
                "test", test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeAdded(":hidden", hidden.getNodeState());
        assertNull(hiddenValidator);
    }

    @Test
    public void hiddenNodeChanged() throws CommitFailedException {
        AccessControlValidatorProvider provider = createValidatorProvider();
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child(":hidden");
        NodeState root = builder.getNodeState();

        NodeBuilder test = root.builder().child("test");
        NodeBuilder hidden = test.child(":hidden");
        hidden.child("added");

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeChanged(
                "test", root.getChildNode("test"), test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeChanged(":hidden", root.getChildNode("test").getChildNode(":hidden"), hidden.getNodeState());
        assertNull(hiddenValidator);
    }

    @Test
    public void hiddenNodeDeleted() throws CommitFailedException {
        AccessControlValidatorProvider provider = createValidatorProvider();
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child(":hidden");
        NodeState root = builder.getNodeState();

        builder = root.builder();
        NodeBuilder test = builder.child("test");
        test.child(":hidden").remove();

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeChanged("test", root.getChildNode("test"), test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeDeleted(
                ":hidden", root.getChildNode("test").getChildNode(":hidden"));
        assertNull(hiddenValidator);
    }

    /**
     * Test case illustrating OAK-8081
     */
    @Test
    public void testRestrictionsUsedByOtherModule() throws Exception {
        AuthorizationConfiguration sc = mock(AuthorizationConfiguration.class);
        // new acNode is covered by Context.definesTree
        when(sc.getContext()).thenReturn(new Context.Default() {
            @Override
            public boolean definesTree(@NotNull Tree tree) {
                return "differentAccessControl".equals(tree.getName());
            }
        });
        when(sc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        SecurityProviderHelper.updateConfig(getSecurityProvider(), sc, AuthorizationConfiguration.class);

        Tree acNode = TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), "differentAccessControl", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        RestrictionProvider rp = new RestrictionProviderImpl();
        Restriction r = rp.createRestriction(PathUtils.ROOT_PATH, REP_ITEM_NAMES, new Value[] {getValueFactory(root).createValue("someName", PropertyType.NAME)});
        rp.writeRestrictions(PathUtils.ROOT_PATH, acNode, Set.of(r));

        root.commit();
    }

    /**
     * Test case illustrating OAK-8081, where a given 'aceTree' is not covered by the authorization-context and thus
     * the AccessControlValidator will still fail.
     */
    @Test(expected = CommitFailedException.class)
    public void testRestrictionsUsedByOtherModule2() throws Exception {
        AuthorizationConfiguration sc = mock(AuthorizationConfiguration.class);
        // new acNode is not covered by Context.definesTree
        when(sc.getContext()).thenReturn(new Context.Default());
        when(sc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        SecurityProviderHelper.updateConfig(getSecurityProvider(), sc, AuthorizationConfiguration.class);

        Tree acNode = TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), "notCoveredByContext", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        RestrictionProvider rp = new RestrictionProviderImpl();
        Restriction r = rp.createRestriction(PathUtils.ROOT_PATH, REP_ITEM_NAMES, new Value[]{getValueFactory(root).createValue("someName", PropertyType.NAME)});
        rp.writeRestrictions(PathUtils.ROOT_PATH, acNode, Set.of(r));

        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 2);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddPolicyTreeWithInvalidName() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        rootTree.setProperty(JCR_MIXINTYPES, ImmutableList.of(MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES);
        TreeUtil.addChild(rootTree, "invalidName", NT_REP_ACL);

        Validator v = createRootValidator(rootTree);
        try {
            v.childNodeAdded("invalidName", mock(NodeState.class));
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 3);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddEntyWithEmptyPrivileges() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree policy = createPolicy(rootTree, false);
        Tree entry = policy.getChild(aceName);
        entry.setProperty(REP_PRIVILEGES, ImmutableList.of(), Type.NAMES);

        Validator v = createRootValidator(rootTree);
        try {
            v.childNodeAdded(policy.getName(), getTreeProvider().asNodeState(policy)).childNodeAdded(entry.getName(), getTreeProvider().asNodeState(entry));
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 9);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddEntyWithNullrivileges() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree policy = createPolicy(rootTree, false);
        Tree entry = policy.getChild(aceName);
        entry.removeProperty(REP_PRIVILEGES);

        Validator v = createRootValidator(rootTree);
        try {
            v.childNodeAdded(policy.getName(), getTreeProvider().asNodeState(policy)).childNodeAdded(entry.getName(), getTreeProvider().asNodeState(entry));
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 9);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddEntyWithEmptyPrincipalName() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree policy = createPolicy(rootTree, false);
        Tree entry = policy.getChild(aceName);
        entry.setProperty(REP_PRINCIPAL_NAME, "");

        Validator v = createRootValidator(rootTree);
        try {
            v.childNodeAdded(policy.getName(), getTreeProvider().asNodeState(policy)).childNodeAdded(entry.getName(), getTreeProvider().asNodeState(entry));
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 8);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddEntyWithNullPrincipalName() throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        Tree policy = createPolicy(rootTree, false);
        Tree entry = policy.getChild(aceName);
        entry.removeProperty(REP_PRINCIPAL_NAME);

        Validator v = createRootValidator(rootTree);
        try {
            v.childNodeAdded(policy.getName(), getTreeProvider().asNodeState(policy)).childNodeAdded(entry.getName(), getTreeProvider().asNodeState(entry));
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.ACCESS_CONTROL, 8);
        }
    }

    //---< additional tests verifying the that type-editor spots invalid sequences of access controlled nodes >---------

    @Test(expected = CommitFailedException.class)
    public void testAddRepoPolicyWithAcl() throws Exception {
        addRepoPolicyWithAcContent(createPolicy(getTestTree(), false));
    }

    @Test(expected = CommitFailedException.class)
    public void testAddRepoPolicyWithAce() throws Exception {
        addRepoPolicyWithAcContent(createPolicy(getTestTree(), false).addChild(aceName));
    }

    @Test(expected = CommitFailedException.class)
    public void testAddRepoPolicyWithRestriction() throws Exception {
        Tree ace = createPolicy(getTestTree(), true).getChild(aceName);
        addRepoPolicyWithAcContent(ace.getChild(REP_RESTRICTIONS));
    }

    private void addRepoPolicyWithAcContent(@NotNull Tree acContent) throws Exception {
        TreeUtil.addChild(acContent, REP_REPO_POLICY, NT_REP_ACL);
        try {
            root.commit();
            fail("Adding an ACL below access control content should fail");
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.CONSTRAINT, 25);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddAceWithAce() throws Exception {
        addAceWithAcContent(createPolicy(getTestTree(), false).getChild(aceName));
    }

    @Test(expected = CommitFailedException.class)
    public void testAddAceWithRestriction() throws Exception {
        Tree ace = createPolicy(getTestTree(), true).getChild(aceName);
        addAceWithAcContent(ace.getChild(REP_RESTRICTIONS));
    }

    private void addAceWithAcContent(@NotNull Tree acContent) throws Exception {
        TreeUtil.addChild(acContent, "invalidACE", NT_REP_DENY_ACE);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.CONSTRAINT, 25);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddRestrictionWithAcl() throws Exception {
        addRestrictionWithAcContent(createPolicy(getTestTree(), false));
    }

    @Test(expected = CommitFailedException.class)
    public void testAddRestrictionWithRestriction() throws Exception {
        Tree ace = createPolicy(getTestTree(), true).getChild(aceName);
        addRestrictionWithAcContent(ace.getChild(REP_RESTRICTIONS));
    }

    private void addRestrictionWithAcContent(@NotNull Tree acContent) throws Exception {
        TreeUtil.addChild(acContent, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.CONSTRAINT, 25);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testInvalidRestrictionWithACE() throws Exception {
        Tree ace = createPolicy(getTestTree(), false).getChild(aceName);
        TreeUtil.addChild(ace, "invalidRestriction", NT_REP_RESTRICTIONS);
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailedException(e, CommitFailedException.CONSTRAINT, 25);
        }
    }
}