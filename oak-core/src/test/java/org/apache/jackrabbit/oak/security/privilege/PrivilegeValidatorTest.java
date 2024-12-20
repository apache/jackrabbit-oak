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
package org.apache.jackrabbit.oak.security.privilege;

import java.util.Collections;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrivilegeValidatorTest extends AbstractSecurityTest implements PrivilegeConstants {

    private PrivilegeBitsProvider bitsProvider;
    private Tree privilegesTree;

    @Before
    public void before() throws Exception {
        super.before();
        bitsProvider = new PrivilegeBitsProvider(root);
        privilegesTree = requireNonNull(bitsProvider.getPrivilegesTree());
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @NotNull
    private Tree createPrivilegeTree(@NotNull String privName, @NotNull String... aggr) {
        Tree privTree = privilegesTree.addChild(privName);
        privTree.setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, Type.NAME);
        privTree.setProperty(REP_AGGREGATES, Set.of(aggr), Type.NAMES);
        return privTree;
    }

    private void register(@NotNull String privName, @NotNull String... aggr) throws Exception {
        getPrivilegeManager(root).registerPrivilege(privName, false, aggr);
    }

    private static void setPrivilegeBits(@NotNull Tree tree, @NotNull String name, long value) {
        tree.setProperty(PropertyStates.createProperty(name, Collections.singleton(value), Type.LONGS));
    }

    @NotNull
    private PrivilegeValidator createPrivilegeValidator() {
        Root immutable = getRootProvider().createReadOnlyRoot(root);
        return new PrivilegeValidator(immutable, immutable, getTreeProvider());
    }

    private static CommitFailedException assertCommitFailed(@NotNull CommitFailedException e, int code) {
        assertTrue(e.isConstraintViolation());
        assertEquals(code, e.getCode());
        return e;
    }

    @Test(expected = CommitFailedException.class)
    public void testMissingPrivilegeBits() throws Exception {
        try {
            createPrivilegeTree("test");
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 21);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testBitsConflict() throws Exception {
        try {
            Tree privTree = createPrivilegeTree("test");
            bitsProvider.getBits(JCR_READ).writeTo(privTree);
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 49);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testBitsConflictWithAggregation() throws Exception {
        try {
            Tree privTree = createPrivilegeTree("test");
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES,
                    ImmutableList.of(JCR_READ, JCR_MODIFY_PROPERTIES), Type.NAMES));
            setPrivilegeBits(privTree, REP_BITS, 340);

            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 53);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testNextNotUpdated() throws Exception{
        try {
            Tree privTree = createPrivilegeTree("test");
            PrivilegeBits.getInstance(privilegesTree).writeTo(privTree);

            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 43);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testChangeNext() throws Exception {
        try {
            setPrivilegeBits(bitsProvider.getPrivilegesTree(), REP_NEXT, 1);
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 43);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testSingularAggregation() throws Exception {
        try {
            Tree privTree = createPrivilegeTree("test");
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES, Collections.singletonList(JCR_READ), Type.NAMES));
            PrivilegeBits.getInstance(bitsProvider.getBits(JCR_READ)).writeTo(privTree);

            root.commit();
            fail("Aggregation of a single privilege is invalid.");
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 50);
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2413">OAK-2413</a>
     */
    @Test(expected = CommitFailedException.class)
    public void testChildNodeChangedWithChanges() throws CommitFailedException {
        NodeBuilder nb = EmptyNodeState.EMPTY_NODE.builder();
        nb.setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, Type.NAME);

        NodeState privilegeDefinition = nb.getNodeState();
        assertTrue(NT_REP_PRIVILEGE.equals(NodeStateUtils.getPrimaryTypeName(privilegeDefinition)));

        PrivilegeValidator pv = new PrivilegeValidator(root, root, getTreeProvider());
        try {
            pv.childNodeChanged("test", privilegeDefinition, EmptyNodeState.EMPTY_NODE);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 41);
        }
    }
    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2413">OAK-2413</a>
     */
    @Test
    public void testChildNodeChangedWithoutChanges() throws CommitFailedException {
        NodeBuilder nb = EmptyNodeState.EMPTY_NODE.builder();
        nb.setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, Type.NAME);

        NodeState privilegeDefinition = nb.getNodeState();
        assertEquals(NT_REP_PRIVILEGE, NodeStateUtils.getPrimaryTypeName(privilegeDefinition));

        PrivilegeValidator pv = new PrivilegeValidator(root, root, getTreeProvider());
        assertNull(pv.childNodeChanged("test", privilegeDefinition, privilegeDefinition));
    }

    @Test(expected = CommitFailedException.class)
    public void testAggregatesIncludesJcrAll() throws Exception {
        try {
            Tree privTree = createPrivilegeTree("test");
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(JCR_ALL, JCR_READ, JCR_WRITE), Type.NAMES));
            PrivilegeBits.getInstance(bitsProvider.getBits(JCR_ALL, JCR_READ, JCR_WRITE)).writeTo(privTree);

            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 53);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAggregatesMatchesExisting() throws Exception {
        try {
            Tree privTree = createPrivilegeTree("test");
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(REP_READ_NODES, REP_READ_PROPERTIES), Type.NAMES));
            PrivilegeBits.getInstance(bitsProvider.getBits(REP_READ_NODES, REP_READ_PROPERTIES)).writeTo(privTree);

            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 53);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testPropertyChanged() throws Exception {
        try {
            PropertyState before = PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(REP_READ_NODES, REP_READ_PROPERTIES), Type.NAMES);
            PropertyState after = PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(REP_READ_NODES), Type.NAMES);

            PrivilegeValidator validator = new PrivilegeValidator(root, root, getTreeProvider());
            validator.propertyChanged(before, after);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 45);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testPropertyDeleted() throws Exception {
        try {
            PropertyState before = PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(REP_READ_NODES, REP_READ_PROPERTIES), Type.NAMES);

            PrivilegeValidator validator = new PrivilegeValidator(root, root, getTreeProvider());
            validator.propertyDeleted(before);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 46);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testChildNodeDeleted() throws Exception {
        try {
            root.getTree(PRIVILEGES_PATH).getChild(JCR_READ).remove();
            root.commit();
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 42);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testPrivBitsMissing() throws Exception{
        try {
            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .getNodeState();

            PrivilegeValidator validator = createPrivilegeValidator();
            validator.childNodeAdded("test", newDef);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 48);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testUnknownAggregate() throws Exception {
        try {
            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .setProperty(REP_BITS, 8)
                    .setProperty(REP_AGGREGATES, ImmutableList.of("unknown", JCR_READ), Type.NAMES)
                    .getNodeState();

            PrivilegeValidator validator = createPrivilegeValidator();
            validator.childNodeAdded("test", newDef);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 51);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testCircularAggregate() throws Exception {
        try {
            register("test");

            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .setProperty(REP_BITS, 8)
                    .setProperty(REP_AGGREGATES, ImmutableList.of("test", JCR_READ), Type.NAMES)
                    .getNodeState();

            PrivilegeValidator validator = createPrivilegeValidator();
            validator.childNodeAdded("test", newDef);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 52);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testCircularAggregate2() throws Exception {
        try {
            register("test");
            register("test2", "test", PrivilegeConstants.JCR_READ);

            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .setProperty(REP_BITS, 8)
                    .setProperty(REP_AGGREGATES, ImmutableList.of("test2", JCR_READ), Type.NAMES)
                    .getNodeState();

            PrivilegeValidator validator = createPrivilegeValidator();
            validator.childNodeAdded("test", newDef);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 52);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testInvalidAggregation() throws Exception {
        Root before = adminSession.getLatestRoot();
        Tree defsBefore = before.getTree(PRIVILEGES_PATH);
        defsBefore.getChild(REP_READ_NODES).remove();

        Tree privDefs = root.getTree(PRIVILEGES_PATH);
        Tree newPriv = TreeUtil.addChild(privDefs, "newPriv", NT_REP_PRIVILEGE);
        PrivilegeBits.getInstance(PrivilegeBits.BUILT_IN.get(JCR_READ), PrivilegeBits.BUILT_IN.get(JCR_ADD_CHILD_NODES)).writeTo(newPriv);
        newPriv.setProperty(REP_AGGREGATES, ImmutableList.of(JCR_READ, JCR_ADD_CHILD_NODES), Type.NAMES);

        TreeProvider tp = mock(TreeProvider.class);
        when(tp.createReadOnlyTree(any(Tree.class), anyString(), any(NodeState.class))).thenReturn(newPriv);
        try {
            PrivilegeValidator validator = new PrivilegeValidator(before, root, tp);
            validator.childNodeAdded("newPriv", getTreeProvider().asNodeState(newPriv));
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 47);
        }
    }

    @Test
    public void testOtherNodeAdded() throws Exception {
        NodeState ns = mock(NodeState.class);
        PrivilegeValidator validator = createPrivilegeValidator();
        assertNull(validator.childNodeAdded("test", ns));

        when(ns.getProperty(JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME));
        assertNull(validator.childNodeAdded("test", ns));
    }

    @Test
    public void testOtherNodeChanged() throws Exception {
        NodeState ns = mock(NodeState.class);
        PrivilegeValidator validator = createPrivilegeValidator();
        assertNull(validator.childNodeChanged("test", ns, ns));

        when(ns.getProperty(JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME));
        assertNull(validator.childNodeChanged("test", ns, ns));
    }

    @Test
    public void testOtherNodeDeleted() throws Exception {
        NodeState ns = mock(NodeState.class);
        PrivilegeValidator validator = createPrivilegeValidator();
        assertNull(validator.childNodeDeleted("test", ns));

        when(ns.getProperty(JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME));
        assertNull(validator.childNodeDeleted("test", ns));
    }

    @Test(expected = CommitFailedException.class)
    public void testNonExistingPrivilegeRoot() throws Exception {
        Tree t = when(mock(Tree.class).exists()).thenReturn(false).getMock();
        Root r = when(mock(Root.class).getTree(PRIVILEGES_PATH)).thenReturn(t).getMock();
        PrivilegeValidator validator = new PrivilegeValidator(r, r, getTreeProvider());
        try {
            PropertyState ps = PropertyStates.createProperty(REP_NEXT, "any");
            validator.propertyChanged(ps, ps);
        } catch (CommitFailedException e) {
            throw assertCommitFailed(e, 44);
        }
    }

    @Test
    public void testPropertyAdded() {
        PrivilegeValidator validator = createPrivilegeValidator();
        validator.propertyAdded(mock(PropertyState.class));
    }
}
