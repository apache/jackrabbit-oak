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

import com.google.common.collect.ImmutableList;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
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

    private Tree createPrivilegeTree() {
        Tree privTree = privilegesTree.addChild("test");
        privTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGE, Type.NAME);
        return privTree;
    }

    private static void setPrivilegeBits(Tree tree, String name, long value) {
        tree.setProperty(PropertyStates.createProperty(name, Collections.singleton(value), Type.LONGS));
    }

    @Test
    public void testMissingPrivilegeBits() {
        try {
            createPrivilegeTree();
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
            Tree privTree = createPrivilegeTree();
            bitsProvider.getBits(JCR_READ).writeTo(privTree);
            root.commit();
            fail("Conflicting privilege bits property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals("OakConstraint0049: PrivilegeBits already in used.", e.getMessage());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testBitsConflictWithAggregation() {
        try {
            Tree privTree = createPrivilegeTree();
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES,
                    ImmutableList.of(JCR_READ, JCR_MODIFY_PROPERTIES), Type.NAMES));
            setPrivilegeBits(privTree, REP_BITS, 340);

            root.commit();
            fail("Privilege bits don't match the aggregation.");
        } catch (CommitFailedException e) {
            // success
            assertEquals("OakConstraint0053: Invalid privilege bits for aggregated privilege definition.", e.getMessage());
        } finally {
            root.refresh();
        }


    }

    @Test
    public void testNextNotUpdated() {
        try {
            Tree privTree = createPrivilegeTree();
            PrivilegeBits.getInstance(privilegesTree).writeTo(privTree);

            root.commit();
            fail("Outdated rep:next property must be detected.");
        } catch (CommitFailedException e) {
            // success
            assertEquals("OakConstraint0043: Next bits not updated", e.getMessage());
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
            assertEquals("OakConstraint0043: Next bits not updated", e.getMessage());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testSingularAggregation() {
        try {
            Tree privTree = createPrivilegeTree();
            privTree.setProperty(PropertyStates.createProperty(REP_AGGREGATES, Collections.singletonList(JCR_READ), Type.NAMES));
            PrivilegeBits.getInstance(bitsProvider.getBits(JCR_READ)).writeTo(privTree);

            root.commit();
            fail("Aggregation of a single privilege is invalid.");
        } catch (CommitFailedException e) {
            // success
            assertEquals("OakConstraint0050: Singular aggregation is equivalent to existing privilege.", e.getMessage());
        } finally {
            root.refresh();
        }
    }
}