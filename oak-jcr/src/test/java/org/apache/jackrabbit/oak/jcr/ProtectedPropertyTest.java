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
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

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
            test.setProperty(Property.JCR_CREATED, false);
            session.save();
            test.addMixin(NodeType.MIX_CREATED);
            session.save();
            assertEquals(false, test.getProperty(Property.JCR_CREATED).getBoolean());
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
            test.setProperty(Property.JCR_UUID, testUuid);
            session.save();
            test.addMixin(NodeType.MIX_REFERENCEABLE);
            session.save();
            // JCR spec
            // (https://developer.adobe.com/experience-manager/reference-materials/spec/jcr/2.0/3_Repository_Model.html#3.8%20Referenceable%20Nodes)
            // requests an "auto-created" property, so it might be a surprise
            // that Oak actually keeps the application-assigned previous value.
            assertEquals(testUuid, test.getProperty(Property.JCR_UUID).getString());
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
            String testUuid = ref.getProperty(Property.JCR_UUID).getString();
            test.setProperty(Property.JCR_UUID, testUuid);
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
            test.setProperty(Property.JCR_UUID, testUuid);
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
    public void setSameUuidOnTwoNtUnstructuredNodes() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        Node test2 = testNode.addNode(TEST_NODE_NAME + "2", NodeType.NT_UNSTRUCTURED);
        session.save();
        try {
            String testUuid = UUID.randomUUID().toString();
            test.setProperty(Property.JCR_UUID, testUuid);
            test2.setProperty(Property.JCR_UUID, testUuid);
            session.save();
            fail("should not allow the same UUID on two different nodes");
        } catch (ConstraintViolationException ex) {
            // expected
        } finally {
            test2.remove();
            test.remove();
            session.save();
        }
    }

    @Test
    public void setSameUuidOnTwoNtUnstructuredNodesTwoSessions() throws RepositoryException {
        Session session2 = createAdminSession();
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        Node test2 = session2.getNode(testNode.getParent().getPath()).addNode(TEST_NODE_NAME + "2", NodeType.NT_UNSTRUCTURED);
        session.save();
        session2.save();
        try {
            String testUuid = UUID.randomUUID().toString();
            test.setProperty(Property.JCR_UUID, testUuid);
            test2.setProperty(Property.JCR_UUID, testUuid);
            session.save();
            session2.save();
            fail("should not allow the same UUID on two different nodes");
        } catch (ConstraintViolationException ex) {
            // expected
        } finally {
            test2.remove();
            test.remove();
            session.save();
            session2.logout();
        }
    }

    @Test
    public void jcrMixinReferenceableOnNtUnstructuredAfterRemovingMixin() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        test.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();
        try {
            // check jcr:uuid is there
            String prevUuid = test.getProperty(Property.JCR_UUID).getString();
            test.removeMixin(NodeType.MIX_REFERENCEABLE);
            session.save();
            // ist jcr:uuid gone now?
            try {
                String newUuid = test.getProperty(Property.JCR_UUID).getString();
                fail("jcr:uuid should be gone after removing the mixin type, was " + prevUuid + ", now is " + newUuid);
            } catch (PathNotFoundException ex) {
                // expected
            }
            String testUuid = UUID.randomUUID().toString();
            test.setProperty(Property.JCR_UUID, testUuid);
            session.save();
            test.addMixin(NodeType.MIX_REFERENCEABLE);
            session.save();
            assertEquals(testUuid, test.getProperty(Property.JCR_UUID).getString());
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
            test.setProperty(Property.JCR_UUID, testUuid);
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
            updateJcrUuidUsingRemoveMixin(test, newUuid);
            assertEquals(newUuid, test.getProperty(Property.JCR_UUID).getString());
            session.save();
            assertEquals(newUuid, test.getProperty(Property.JCR_UUID).getString());
            assertTrue(test.isSame(session.getNodeByIdentifier(newUuid)));
        } finally {
            ref.remove();
            test.remove();
            session.save();
        }
    }

    @Test
    public void changeUuidOnReferencedNodeWithOnlyMixin2Sessions() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_UNSTRUCTURED);
        test.addMixin(NodeType.MIX_REFERENCEABLE);
        session.save();
        Node ref = testNode.addNode(TEST_NODE_NAME_REF, NodeType.NT_UNSTRUCTURED);
        ref.setProperty("reference", test.getIdentifier(), PropertyType.REFERENCE);
        session.save();

        Session session2 = createAdminSession();
        Node testNode2 = session2.getNode(testNode.getPath());
        Node test2 = testNode2.addNode(TEST_NODE_NAME + "2", NodeType.NT_UNSTRUCTURED);
        test2.addMixin(NodeType.MIX_REFERENCEABLE);
        session2.save();
        Node ref2 = testNode.addNode(TEST_NODE_NAME_REF + "2", NodeType.NT_UNSTRUCTURED);
        ref2.setProperty("reference", test2.getIdentifier(), PropertyType.REFERENCE);
        session2.save();

        try {
            String newUuid = UUID.randomUUID().toString();
            updateJcrUuidUsingRemoveMixin(test, newUuid);
            updateJcrUuidUsingRemoveMixin(test2, newUuid);
            assertEquals(newUuid, test.getProperty(Property.JCR_UUID).getString());
            assertEquals(newUuid, test2.getProperty(Property.JCR_UUID).getString());
            session.save();
            assertTrue(test.isSame(session.getNodeByIdentifier(newUuid)));
            session2.save();
            fail("saving 2nd session should fail");
            // SEGMENT_OK fails with the former, DOCUMENT_NS with the latter
        } catch (ConstraintViolationException | ReferentialIntegrityException ex) {
            // expected
        } finally {
            ref2.remove();
            test2.remove();
            ref.remove();
            test.remove();
            session2.logout();
        }
    }

    @Test
    public void changeUuidOnReferencedNodeWithInheritedMixin() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_RESOURCE);
        test.setProperty(Property.JCR_DATA, session.getValueFactory().createBinary(new ByteArrayInputStream(new byte[0])));
        session.save();
        Node ref = testNode.addNode(TEST_NODE_NAME_REF, NodeType.NT_UNSTRUCTURED);
        ref.setProperty("reference", test.getIdentifier(), PropertyType.REFERENCE);
        session.save();

        try {
            String newUuid = UUID.randomUUID().toString();
            updateJcrUuidUsingRemoveMixin(test, newUuid);
            fail("removing mixin:referenceable should fail on nt:resource");
        } catch (NoSuchNodeTypeException ex) {
            // expected
        } finally {
            ref.remove();
            test.remove();
            session.save();
        }
    }

    @Test
    public void changeUuidOnReferencedNodeWithInheritedMixinByChangingNodeTypeTemporarily() throws RepositoryException {
        Node test = testNode.addNode(TEST_NODE_NAME, NodeType.NT_RESOURCE);
        test.setProperty(Property.JCR_DATA, session.getValueFactory().createBinary(new ByteArrayInputStream(new byte[0])));
        session.save();
        Node ref = testNode.addNode(TEST_NODE_NAME_REF, NodeType.NT_UNSTRUCTURED);
        ref.setProperty("reference", test.getIdentifier(), PropertyType.REFERENCE);
        session.save();

        try {
            String newUuid = UUID.randomUUID().toString();
            updateJcrUuidUsingNodeTypeManager(test, newUuid);
            assertEquals(newUuid, test.getProperty(Property.JCR_UUID).getString());
            session.save();
            assertEquals(newUuid, test.getProperty(Property.JCR_UUID).getString());
            assertTrue(test.isSame(session.getNodeByIdentifier(newUuid)));
        } finally {
            ref.remove();
            test.remove();
            session.save();
        }
    }

    private static void updateJcrUuidUsingRemoveMixin(Node target, String newUUID) throws RepositoryException {
        // temporary node for rewriting the references
        Node tmp = target.getParent().addNode(TEST_NODE_NAME_TMP, NodeType.NT_UNSTRUCTURED);
        tmp.addMixin(NodeType.MIX_REFERENCEABLE);

        try {
            // find all existing references to the node for which we want to rewrite the jcr:uuid
            Set<Property> referrers = getReferrers(target);

            // move existing references to TEST_MODE_NAME to TEST_NODE_NAME_TMP
            setReferrersTo(referrers, tmp.getIdentifier());

            // rewrite jcr:uuid
            target.removeMixin(NodeType.MIX_REFERENCEABLE);
            target.setProperty(Property.JCR_UUID, newUUID);
            target.addMixin(NodeType.MIX_REFERENCEABLE);

            // restore references
            setReferrersTo(referrers, newUUID);
        } finally {
            tmp.remove();
        }
    }

    private static void updateJcrUuidUsingNodeTypeManager(Node target, String newUUID) throws RepositoryException {
        // temporary node for rewriting the references
        Node tmp = target.getParent().addNode(TEST_NODE_NAME_TMP, NodeType.NT_UNSTRUCTURED);
        tmp.addMixin(NodeType.MIX_REFERENCEABLE);

        try {
            String previousType = target.getPrimaryNodeType().getName();

            // find all existing references to the node for which we want to rewrite the jcr:uuid
            Set<Property> referrers = getReferrers(target);

            // move existing references to TEST_MODE_NAME to TEST_NODE_NAME_TMP
            setReferrersTo(referrers, tmp.getIdentifier());

            // rewrite jcr:uuid
            String temporaryType = registerPrimaryTypeExtendingAndUnprotectingJcrUUID(target);
            target.setPrimaryType(temporaryType);
            target.setProperty(Property.JCR_UUID, newUUID);
            target.setPrimaryType(previousType);
            unregisterPrimaryTypeExtendingAndUnprotectingJcrUUID(target, temporaryType);

            // restore references
            setReferrersTo(referrers, newUUID);

            // assert temporary node type is gone
            try {
                target.getSession().getWorkspace().getNodeTypeManager().getNodeType(temporaryType);
                fail("temporary node type should be removed");
            } catch (NoSuchNodeTypeException ex) {
                // expected
            }
        } finally {
            tmp.remove();
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

    @SuppressWarnings("unchecked")
    private static String registerPrimaryTypeExtendingAndUnprotectingJcrUUID(Node node) throws RepositoryException {
        String tmpNodeTypeName = "tmp-" + UUID.randomUUID().toString();
        NodeTypeManager ntMgr = node.getSession().getWorkspace().getNodeTypeManager();

        NodeTypeTemplate unprotectedNTT = ntMgr.createNodeTypeTemplate();
        unprotectedNTT.setName(tmpNodeTypeName);
        unprotectedNTT.setDeclaredSuperTypeNames(new String[] {node.getPrimaryNodeType().getName()});
        PropertyDefinitionTemplate pdt = ntMgr.createPropertyDefinitionTemplate();
        pdt.setName(Property.JCR_UUID);
        pdt.setProtected(false);
        unprotectedNTT.getPropertyDefinitionTemplates().add(pdt);
        ntMgr.registerNodeType(unprotectedNTT, true);

        return tmpNodeTypeName;
    }

    private static void unregisterPrimaryTypeExtendingAndUnprotectingJcrUUID(Node node, String tmpType) throws RepositoryException {
        NodeTypeManager ntMgr = node.getSession().getWorkspace().getNodeTypeManager();

        ntMgr.unregisterNodeType(tmpType);
    }
}
