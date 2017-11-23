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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeTemplate;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CugValidatorTest extends AbstractCugTest {

    private NodeUtil node;

    @Override
    public void before() throws Exception {
        super.before();

        node = new NodeUtil(root.getTree(SUPPORTED_PATH));
    }

    @Test
    public void testChangePrimaryType() {
        node = new NodeUtil(root.getTree(SUPPORTED_PATH2));
        try {
            node.setName(JcrConstants.JCR_PRIMARYTYPE, NT_REP_CUG_POLICY);
            node.setStrings(REP_PRINCIPAL_NAMES, EveryonePrincipal.NAME);
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(20, e.getCode());
        }
    }

    @Test
    public void testChangePrimaryTypeOfCug() throws Exception {
        node.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_CUG_MIXIN);
        NodeUtil cug = node.addChild(REP_CUG_POLICY, NT_REP_CUG_POLICY);
        cug.setStrings(REP_PRINCIPAL_NAMES, EveryonePrincipal.NAME);
        root.commit();

        try {
            cug.setName(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(21, e.getCode());
        }
    }

    @Test
    public void testInvalidPrimaryType() throws Exception {
        NodeUtil cug = node.addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        cug.setStrings(REP_PRINCIPAL_NAMES, EveryonePrincipal.NAME);

        try {
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(21, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testMissingMixin() throws Exception {
        NodeUtil cug = node.addChild(REP_CUG_POLICY, NT_REP_CUG_POLICY);
        cug.setStrings(REP_PRINCIPAL_NAMES, EveryonePrincipal.NAME);

        try {
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(22, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testRemoveMixin() throws Exception {
        node.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_CUG_MIXIN);
        NodeUtil cug = node.addChild(REP_CUG_POLICY, NT_REP_CUG_POLICY);
        cug.setStrings(REP_PRINCIPAL_NAMES, EveryonePrincipal.NAME);
        root.commit();

        try {
            node.removeProperty(JcrConstants.JCR_MIXINTYPES);
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(22, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testCugPolicyWithDifferentName() throws Exception {
        node.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_CUG_MIXIN);
        NodeUtil cug = node.addChild("anotherName", NT_REP_CUG_POLICY);
        cug.setStrings(REP_PRINCIPAL_NAMES, EveryonePrincipal.NAME);
        try {
            root.commit();
            fail();
        }  catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertEquals(23, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testNodeTypeWithCugNames() throws Exception {
        ReadWriteNodeTypeManager ntMgr = new ReadWriteNodeTypeManager() {
            @Nonnull
            @Override
            protected Root getWriteRoot() {
                return root;
            }

            @CheckForNull
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
}