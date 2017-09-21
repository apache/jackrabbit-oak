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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import javax.annotation.Nonnull;
import javax.jcr.nodetype.NodeTypeTemplate;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JcrUUIDTest extends AbstractOakCoreTest {

    private static final String NT_NAME = "referenceableTestNodeType";

    private ReadWriteNodeTypeManager ntMgr;
    private String referenceablePath;

    @Before
    public void before() throws Exception {
        super.before();

        ntMgr = new ReadWriteNodeTypeManager() {
            @Nonnull
            @Override
            protected Root getWriteRoot() {
                return root;
            }

            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }
        };
        if (!ntMgr.hasNodeType(NT_NAME)) {
            NodeTypeTemplate tmpl = ntMgr.createNodeTypeTemplate();
            tmpl.setName(NT_NAME);
            tmpl.setDeclaredSuperTypeNames(new String[]{JcrConstants.MIX_REFERENCEABLE, JcrConstants.NT_UNSTRUCTURED});
            ntMgr.registerNodeType(tmpl, true);
        }

        NodeUtil a = new NodeUtil(root.getTree("/a"));
        NodeUtil test = a.addChild("referenceable", NT_NAME);
        test.setString(JcrConstants.JCR_UUID, IdentifierManager.generateUUID());
        referenceablePath = test.getTree().getPath();
    }

    @Override
    protected Oak withEditors(Oak oak) {
        return oak.with(new TypeEditorProvider());
    }

    /**
     * Creating a tree which is referenceable doesn't require any property
     * related privilege to be granted as the jcr:uuid property is defined to
     * be autocreated and protected.
     */
    @Test
    public void testCreateJcrUuid() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        Root testRoot = getTestRoot();
        testRoot.refresh();

        NodeUtil a = new NodeUtil(testRoot.getTree("/a"));
        NodeUtil test = a.addChild("referenceable2", NT_NAME);
        test.setString(JcrConstants.JCR_UUID, IdentifierManager.generateUUID());
        testRoot.commit();
    }

    /**
     * Creating a referenceable tree with an invalid jcr:uuid must fail.
     */
    @Test
    public void testCreateInvalidJcrUuid() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        try {
            Root testRoot = getTestRoot();
            testRoot.refresh();

            NodeUtil a = new NodeUtil(testRoot.getTree("/a"));
            NodeUtil test = a.addChild("referenceable2", NT_NAME);
            test.setString(JcrConstants.JCR_UUID, "not a uuid");
            testRoot.commit();
            fail("Creating a referenceable node with an invalid uuid must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(12, e.getCode());
        }
    }

    /**
     * Creating a referenceable tree with an invalid jcr:uuid must fail.
     */
    @Test
    public void testCreateBooleanJcrUuid() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        try {
            Root testRoot = getTestRoot();
            testRoot.refresh();

            NodeUtil a = new NodeUtil(testRoot.getTree("/a"));
            NodeUtil test = a.addChild("referenceable2", NT_NAME);
            test.setBoolean(JcrConstants.JCR_UUID, false);
            testRoot.commit();
            fail("Creating a referenceable node with an boolean uuid must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

    /**
     * Creating a non-referenceable tree with an jcr:uuid must fail
     * with AccessDeniedException unless the REP_ADD_PROPERTY privilege
     * is granted
     */
    @Test
    public void testCreateNonReferenceableJcrUuid() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        try {
            Root testRoot = getTestRoot();
            NodeUtil a = new NodeUtil(testRoot.getTree("/a"));
            a.setString(JCR_UUID, IdentifierManager.generateUUID());
            testRoot.commit();
            fail("Creating a jcr:uuid property for an unstructured node without ADD_PROPERTY permission must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
    }

    /**
     * Modifying the jcr:uuid property must fail due to constraint violations.
     */
    @Test
    public void testModifyJcrUuid() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);

        try {
            Root testRoot = getTestRoot();
            Tree test = testRoot.getTree(referenceablePath);
            test.setProperty(JCR_UUID, "anothervalue");
            testRoot.commit();
            fail("An attempt to change the jcr:uuid property must fail");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(12, e.getCode());
        }
    }

    /**
     * Creating a non-referenceable tree with a jcr:uuid must fail
     * with AccessDeniedException unless the REP_ADD_PROPERTY privilege
     * is granted
     */
    @Test
    public void testModifyNonReferenceableJcrUuid() throws Exception {
        NodeUtil a = new NodeUtil(root.getTree("/a"));
        a.setString(JCR_UUID, "some-value");
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        try {
            Root testRoot = getTestRoot();
            a = new NodeUtil(testRoot.getTree("/a"));
            assertNotNull(a.getString(JCR_UUID, null));
            a.setString(JCR_UUID, IdentifierManager.generateUUID());
            testRoot.commit();
            fail("Modifying a jcr:uuid property for an unstructured node without MODIFY_PROPERTY permission must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
    }

    /**
     * Removing the jcr:uuid property must fail due to constraint violations.
     */
    @Test
    public void testRemoveJcrUuid() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ);

        try {
            Root testRoot = getTestRoot();
            Tree test = testRoot.getTree(referenceablePath);
            test.removeProperty(JCR_UUID);
            testRoot.commit();
            fail("Removing the jcr:uuid property of a referenceable node must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(22, e.getCode());
        }
    }
}