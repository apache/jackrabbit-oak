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
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class PrivilegeValidatorTest extends AbstractSecurityTest implements PrivilegeConstants {

    PrivilegeBitsProvider bitsProvider;
    Tree privilegesTree;

    @Before
    public void before() throws Exception {
        super.before();
        bitsProvider = new PrivilegeBitsProvider(root);
        privilegesTree = checkNotNull(bitsProvider.getPrivilegesTree());
    }

    private Tree createPrivilegeTree(@Nonnull String privName, @Nonnull String... aggr) {
        Tree privTree = privilegesTree.addChild(privName);
        privTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, Type.NAME);
        privTree.setProperty(REP_AGGREGATES, ImmutableSet.copyOf(aggr), Type.NAMES);
        return privTree;
    }

    private static void setPrivilegeBits(Tree tree, String name, long value) {
        tree.setProperty(PropertyStates.createProperty(name, Collections.singleton(value), Type.LONGS));
    }

    @Test
    public void testMissingPrivilegeBits() {
        try {
            createPrivilegeTree("test");
            root.commit();
            fail("Missing privilege bits property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testBitsConflict() {
        try {
            Tree privTree = createPrivilegeTree("test");
            bitsProvider.getBits(JCR_READ).writeTo(privTree);
            root.commit();
            fail("Conflicting privilege bits property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(49, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testBitsConflictWithAggregation() {
        try {
            Tree privTree = createPrivilegeTree("test");
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES,
                    ImmutableList.of(JCR_READ, JCR_MODIFY_PROPERTIES), Type.NAMES));
            setPrivilegeBits(privTree, REP_BITS, 340);

            root.commit();
            fail("Privilege bits don't match the aggregation.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(53, e.getCode());
        } finally {
            root.refresh();
        }


    }

    @Test
    public void testNextNotUpdated() {
        try {
            Tree privTree = createPrivilegeTree("test");
            PrivilegeBits.getInstance(privilegesTree).writeTo(privTree);

            root.commit();
            fail("Outdated rep:next property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(43, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testChangeNext() {
        try {
            setPrivilegeBits(bitsProvider.getPrivilegesTree(), REP_NEXT, 1);
            root.commit();
            fail("Outdated rep:next property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(43, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testSingularAggregation() {
        try {
            Tree privTree = createPrivilegeTree("test");
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES, Collections.singletonList(JCR_READ), Type.NAMES));
            PrivilegeBits.getInstance(bitsProvider.getBits(JCR_READ)).writeTo(privTree);

            root.commit();
            fail("Aggregation of a single privilege is invalid.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(50, e.getCode());
        } finally {
            root.refresh();
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2413">OAK-2413</a>
     */
    @Test
    public void testChildNodeChangedWithChanges() throws CommitFailedException {
        NodeBuilder nb = EmptyNodeState.EMPTY_NODE.builder();
        nb.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, Type.NAME);

        NodeState privilegeDefinition = nb.getNodeState();
        assertTrue(NT_REP_PRIVILEGE.equals(NodeStateUtils.getPrimaryTypeName(privilegeDefinition)));

        PrivilegeValidator pv = new PrivilegeValidator(root, root);
        try {
            pv.childNodeChanged("test", privilegeDefinition, EmptyNodeState.EMPTY_NODE);
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(41, e.getCode());
        }
    }
    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2413">OAK-2413</a>
     */
    @Test
    public void testChildNodeChangedWithoutChanges() throws CommitFailedException {
        NodeBuilder nb = EmptyNodeState.EMPTY_NODE.builder();
        nb.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, Type.NAME);

        NodeState privilegeDefinition = nb.getNodeState();
        assertTrue(NT_REP_PRIVILEGE.equals(NodeStateUtils.getPrimaryTypeName(privilegeDefinition)));

        PrivilegeValidator pv = new PrivilegeValidator(root, root);
        assertNull(pv.childNodeChanged("test", privilegeDefinition, privilegeDefinition));
    }

    @Test
    public void testAggregatesIncludesJcrAll() throws Exception {
        try {
            Tree privTree = createPrivilegeTree("test");
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(JCR_ALL, JCR_READ, JCR_WRITE), Type.NAMES));
            PrivilegeBits.getInstance(bitsProvider.getBits(JCR_ALL, JCR_READ, JCR_WRITE)).writeTo(privTree);

            root.commit();
            fail("Aggregation containing jcr:all is invalid.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(53, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testPropertyChanged() throws Exception {
        try {
            PropertyState before = PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(REP_READ_NODES, REP_READ_PROPERTIES), Type.NAMES);
            PropertyState after = PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(REP_READ_NODES), Type.NAMES);

            PrivilegeValidator validator = new PrivilegeValidator(root, root);
            validator.propertyChanged(before, after);
            fail("modifying property in privilege store must fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(45, e.getCode());
        }
    }

    @Test
    public void testPropertyDeleted() throws Exception {
        try {
            PropertyState before = PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(REP_READ_NODES, REP_READ_PROPERTIES), Type.NAMES);

            PrivilegeValidator validator = new PrivilegeValidator(root, root);
            validator.propertyDeleted(before);
            fail("removing property from privilege store must fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(46, e.getCode());
        }
    }

    @Test
    public void testChildNodeDeleted() {
        try {
            root.getTree(PRIVILEGES_PATH).getChild(JCR_READ).remove();
            root.commit();
            fail("removing privilege from privilege store must fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isConstraintViolation());
            assertEquals(42, e.getCode());
        }
    }

    @Test
    public void testPrivBitsMissing() {
        try {
            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .getNodeState();

            PrivilegeValidator validator = new PrivilegeValidator(root, root);
            validator.childNodeAdded("test", newDef);
            fail("missing priv bits must be detected.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(48, e.getCode());
        }
    }

    @Test
    public void testUnknownAggregate() {
        try {
            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .setProperty(REP_BITS, 8)
                    .setProperty(REP_AGGREGATES, ImmutableList.of("unknown", JCR_READ), Type.NAMES)
                    .getNodeState();

            PrivilegeValidator validator = new PrivilegeValidator(root, root);
            validator.childNodeAdded("test", newDef);
            fail("unknown aggregate must be detected.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(51, e.getCode());
        }
    }

    @Test
    public void testCircularAggregate() {
        try {
            createPrivilegeTree("test");

            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .setProperty(REP_BITS, 8)
                    .setProperty(REP_AGGREGATES, ImmutableList.of("test", JCR_READ), Type.NAMES)
                    .getNodeState();

            PrivilegeValidator validator = new PrivilegeValidator(root, root);
            validator.childNodeAdded("test", newDef);
            fail("unknown aggregate must be detected.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(52, e.getCode());
        }
    }

    @Test
    public void testCircularAggregate2() {
        try {
            createPrivilegeTree("test2", "test");

            NodeState newDef = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE)
                    .setProperty(REP_BITS, 8)
                    .setProperty(REP_AGGREGATES, ImmutableList.of("test2", JCR_READ), Type.NAMES)
                    .getNodeState();

            PrivilegeValidator validator = new PrivilegeValidator(root, root);
            validator.childNodeAdded("test", newDef);
            fail("unknown aggregate must be detected.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(52, e.getCode());
        }
    }
}