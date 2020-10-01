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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeTemplate;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class CugValidatorTest extends AbstractCugTest {

    private Tree node;

    @Override
    public void before() throws Exception {
        super.before();

        node = root.getTree(SUPPORTED_PATH);
    }

    @NotNull
    private Validator createRootValidator(@NotNull NodeState ns) {
        return checkNotNull(new CugValidatorProvider().getRootValidator(ns, ns, new CommitInfo("sid", "uid")));
    }

    @Test
    public void testChangePrimaryType() {
        node = root.getTree(SUPPORTED_PATH2);
        try {
            node.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_CUG_POLICY, Type.NAME);
            node.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME), Type.STRINGS);
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(20, e.getCode());
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testPropertyChangedBeforeWasCug() throws CommitFailedException {
        NodeState ns = mock(NodeState.class);
        Validator validator = createRootValidator(ns);
        try {
            PropertyState before = PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_REP_CUG_POLICY);
            PropertyState after = PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED);

            validator.propertyChanged(before, after);
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(20, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testPropertyChangedAfterIsCug() throws CommitFailedException {
        NodeState ns = mock(NodeState.class);
        Validator validator = createRootValidator(ns);
        try {
            PropertyState before = PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED);
            PropertyState after = PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_REP_CUG_POLICY);

            validator.propertyChanged(before, after);
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(20, e.getCode());
            throw e;
        }
    }

    @Test
    public void testPropertyChangedNoCugInvolved() throws Exception {
        NodeState ns = mock(NodeState.class);
        Validator validator = createRootValidator(ns);

        PropertyState before = PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED);
        PropertyState after = PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED);

        validator.propertyChanged(before, after);
    }

    @Test(expected = CommitFailedException.class)
    public void testChangePrimaryTypeOfCug() throws Exception {
        node.setProperty(JcrConstants.JCR_MIXINTYPES, ImmutableList.of(MIX_REP_CUG_MIXIN), Type.NAMES);
        Tree cug = TreeUtil.addChild(node, REP_CUG_POLICY, NT_REP_CUG_POLICY);
        cug.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME), Type.STRINGS);
        root.commit();

        try {
            cug.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);
            root.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(21, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testInvalidPrimaryType() throws Exception {
        Tree cug = TreeUtil.addChild(node, REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        cug.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME), Type.STRINGS);

        try {
            root.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(21, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testMissingMixin() throws Exception {
        Tree cug = TreeUtil.addChild(node, REP_CUG_POLICY, NT_REP_CUG_POLICY);
        cug.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME), Type.STRINGS);

        try {
            root.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(22, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveMixin() throws Exception {
        node.setProperty(JcrConstants.JCR_MIXINTYPES, ImmutableList.of(MIX_REP_CUG_MIXIN), Type.NAMES);
        Tree cug = TreeUtil.addChild(node, REP_CUG_POLICY, NT_REP_CUG_POLICY);
        cug.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME), Type.STRINGS);
        root.commit();

        try {
            node.removeProperty(JcrConstants.JCR_MIXINTYPES);
            root.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(22, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testCugPolicyWithDifferentName() throws Exception {
        node.setProperty(JcrConstants.JCR_MIXINTYPES, ImmutableList.of(MIX_REP_CUG_MIXIN), Type.NAMES);
        Tree cug = TreeUtil.addChild(node, "anotherName", NT_REP_CUG_POLICY);
        cug.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME), Type.STRINGS);
        try {
            root.commit();
        }  catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(23, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testNodeTypeWithCugNames() throws Exception {
        ReadWriteNodeTypeManager ntMgr = new ReadWriteNodeTypeManager() {
            @NotNull
            @Override
            protected Root getWriteRoot() {
                return root;
            }

            @NotNull
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }
        };
        NodeTypeTemplate ntTemplate = ntMgr.createNodeTypeTemplate();
        ntTemplate.setName("testNT");
        NodeDefinitionTemplate ndt = ntMgr.createNodeDefinitionTemplate();
        ndt.setName(REP_CUG_POLICY);
        ndt.setRequiredPrimaryTypeNames(new String[] {JcrConstants.NT_BASE});
        ntTemplate.getNodeDefinitionTemplates().add(ndt);
        ntMgr.registerNodeType(ntTemplate, true);
    }

    @Test(expected = CommitFailedException.class)
    public void testJcrNodeTypesOutsideOfSystemIsValidated() throws Exception {
        Tree n = TreeUtil.addChild(node, JCR_NODE_TYPES, NT_OAK_UNSTRUCTURED);
        Tree cug = TreeUtil.addChild(n, REP_CUG_POLICY, NT_REP_CUG_POLICY);
        cug.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME), Type.STRINGS);

        try {
            root.commit();
        }  catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(22, e.getCode());
            throw e;
        } finally {
            root.refresh();
        }
    }
}
