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

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
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
public class ProtectedPropertyTest extends AbstractRepositoryTest {

    private Session session;
    private Node testNode;
    private static String TEST_NODE_NAME = "ImportOperationsTest";
    private static String TEST_NODE_NAME_REF = "ImportOperationsTest-Reference";
    private static String TEST_NODE_NAME_TMP = "ImportOperationsTest-Temp";

    public ProtectedPropertyTest(NodeStoreFixture fixture) {
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
            assertEquals(false, test.getProperty("jcr:created").getBoolean());
            // in Oak, existing properties are left as-is (even the property
            // type), which means that after adding the mixin:created type, the
            // state of the node might be inconsistent with the mixin:created
            // type. This may come as a surprise, but is allowed as
            // implementation specific behavior, see
            // https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/3_Repository_Model.html#3.7.11.7%20mix:created
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
            // JCR spec
            // (https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/3_Repository_Model.html#3.8%20Referenceable%20Nodes)
            // requests an "auto-created" property, so it might be a surprise
            // that Oak actually keeps the application-assigned previous value.
            assertEquals(testUuid, test.getProperty("jcr:uuid").getString());
        } finally {
            test.remove();
            session.save();
        }
    }

    @Test
    public void jcrMixinReferenceableOnNtUnstructuredBeforeSettingMixinButWithConflict() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        Node ref = testNode.addNode(TEST_NODE_NAME_REF, NodeType.NT_UNSTRUCTURED);
        ref.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();

        try {
            String testUuid = ref.getProperty("jcr:uuid").getString();
            test.setProperty("jcr:uuid", testUuid);
            // note this fails even though test hasn't be set to mix:referenceable
            session.save();
            fail("Attempt so set a UUID already in use should fail");
        } catch (ConstraintViolationException ex) {
            // expected
        } finally {
            test.remove();
            ref.remove();
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

    @Test
    public void changeUuidOnReferencedNodeWithOnlyMixin() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        test.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();
        Node ref = testNode.addNode(TEST_NODE_NAME_REF, NodeType.NT_UNSTRUCTURED);
        ref.setProperty("reference", test.getIdentifier(), PropertyType.REFERENCE);
        session.save();

        try {
            String newUuid = UUID.randomUUID().toString();
            updateJcrUuid(test, newUuid);
        } finally {
            ref.remove();
            test.remove();
            session.save();
        }
    }

    @Test
    public void changeUuidOnReferencedNodeWithInheritedMixin() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_RESOURCE);
        test.setProperty("jcr:data", session.getValueFactory().createBinary(new ByteArrayInputStream(new byte[0])));
        session.save();
        Node ref = testNode.addNode(TEST_NODE_NAME_REF, NodeType.NT_UNSTRUCTURED);
        ref.setProperty("reference", test.getIdentifier(), PropertyType.REFERENCE);
        session.save();

        try {
            String newUuid = UUID.randomUUID().toString();
            updateJcrUuid(test, newUuid);
            fail("removing mixin:referenceable should fail on nt:resource");
        } catch (NoSuchNodeTypeException ex) {
            // expected
        } finally {
            ref.remove();
            test.remove();
            session.save();
        }
    }

    private static void updateJcrUuid(Node target, String newUUID) throws RepositoryException {
        Session session = target.getSession();

        // temporary node for rewriting the references
        Node tmp = target.getParent().addNode(TEST_NODE_NAME_TMP, NodeType.NT_UNSTRUCTURED);
        tmp.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();

        try {
            // find all existing references to the node for which we want to rewrite the jcr:uuid
            Set<Property> referrers = getReferrers(target);

            // move existing references to TEST_MODE_NAME to TEST_NODE_NAME_TMP
            setReferrersTo(referrers, tmp.getIdentifier());
            session.save();

            // rewrite jcr:uuid
            target.removeMixin(NodeType.MIX_REFERENCEABLE);
            target.setProperty("jcr:uuid", newUUID);
            target.addMixin(NodeType.MIX_REFERENCEABLE);
            session.save();

            // restore references
            setReferrersTo(referrers, newUUID);
            session.save();
        } finally {
            tmp.remove();
            session.save();
        }
    }

    private static Set<Property> getReferrers(Node to) throws RepositoryException {
        Set<Property> referrers = new HashSet<>();
        PropertyIterator pit = to.getReferences();
        while (pit.hasNext()) {
            referrers.add(pit.nextProperty());
        }
        return referrers;
    }

    private static void setReferrersTo(Set<Property> referrers, String identifier) throws RepositoryException {
        for (Property p : referrers) {
            // add case for multivalued
            p.getParent().setProperty(p.getName(), identifier);
        }
    }
}
