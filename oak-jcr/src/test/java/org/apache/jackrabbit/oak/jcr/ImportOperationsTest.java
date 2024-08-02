/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Before;
import org.junit.Test;

/**
 * Test coverage for common JCR operations related to importing content.
 * <p>
 * Note that the purpose of these tests is not to check conformance with the JCR
 * specification, but to observe the actual behavior of the implementation
 * (which may be hard to change).
 */
public class ImportOperationsTest extends AbstractRepositoryTest {

    private Session session;
    private Node testNode;
    private static String TEST_NODE_NAME = "ImportOperationsTest";
    private static String TEST_NODE_NAME_REF = "ImportOperationsTest-Reference";

    public ImportOperationsTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        this.session = getAdminSession();
        this.testNode = session.getRootNode().addNode("import-tests", NodeType.NT_UNSTRUCTURED);
        session.save();
    }

    @Test
    public void jcrMixinCreatedOnNtUnstructured() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        try {
            test.setProperty("jcr:created", false);
            session.save();
            test.addMixin(NodeType.MIX_CREATED);
            session.save();
            // surprise: setting the mixin succeeds, and the subsequent state of
            // the node is inconsistent with the mixin's type definition
            assertEquals(false, test.getProperty("jcr:created").getBoolean());
        } finally {
            test.remove();
            session.save();
        }
    }

    @Test
    public void jcrMixinReferenceableOnNtUnstructuredBeforeSettingMixin() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        try {
            String testUuid = UUID.randomUUID().toString();
            test.setProperty("jcr:uuid", testUuid);
            session.save();
            test.addMixin(NodeType.MIX_REFERENCEABLE);
            session.save();
            // surprise: setting the mixin succeeds, and the subsequent state of
            // the node doesn't have an autocreated UUID
            assertEquals(testUuid, test.getProperty("jcr:uuid").getString());
        } finally {
            test.remove();
            session.save();
        }
    }

    @Test
    public void jcrMixinReferenceableOnNtUnstructuredAfterSettingMixin() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        test.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();
        try {
            String testUuid = UUID.randomUUID().toString();
            test.setProperty("jcr:uuid", testUuid);
            session.save();
            fail("Setting jcr:uuid after adding mixin:referenceable should fail");
        } catch (ConstraintViolationException ex) {
            // expected
        } finally {
            test.remove();
            session.save();
        }
    }

    @Test
    public void jcrMixinReferenceableOnNtUnstructuredAfterRemovingMixin() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        test.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();
        try {
            test.removeMixin(NodeType.MIX_REFERENCEABLE);
            String testUuid = UUID.randomUUID().toString();
            test.setProperty("jcr:uuid", testUuid);
            session.save();
            test.addMixin(NodeType.MIX_REFERENCEABLE);
            session.save();
            assertEquals(testUuid, test.getProperty("jcr:uuid").getString());
            Node check = session.getNodeByIdentifier(testUuid);
            assertTrue(test.isSame(check));
        } finally {
            test.remove();
            session.save();
        }
    }

    @Test
    public void jcrMixinReferenceableOnNtUnstructuredAfterRemovingMixinButDanglingReference() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        test.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();
        Node ref = testNode.addNode(TEST_NODE_NAME_REF, NodeType.NT_UNSTRUCTURED);
        ref.setProperty("reference", test.getIdentifier(), PropertyType.REFERENCE);
        session.save();

        try {
            test.removeMixin(NodeType.MIX_REFERENCEABLE);
            String testUuid = UUID.randomUUID().toString();
            test.setProperty("jcr:uuid", testUuid);
            session.save();
            fail("Changing jcr:uuid causing a dangling refence should fail");
        } catch (ReferentialIntegrityException ex) {
            // expected
        } finally {
            ref.remove();
            test.remove();
            session.save();
        }
    }
}
