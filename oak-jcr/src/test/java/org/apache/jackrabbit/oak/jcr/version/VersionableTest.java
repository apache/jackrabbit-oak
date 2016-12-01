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
package org.apache.jackrabbit.oak.jcr.version;

import javax.annotation.Nullable;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import com.google.common.base.Function;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Lists.transform;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;

/**
 * {@code VersionableTest} contains tests for method relevant to
 * versionable nodes.
 */
public class VersionableTest extends AbstractJCRTest {

    public void testGetTypeOfPredecessors() throws RepositoryException {
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        node.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkin(node.getPath());
        assertEquals(PropertyType.nameFromValue(PropertyType.REFERENCE),
                PropertyType.nameFromValue(node.getProperty(jcrPredecessors).getType()));
    }

    public void testReadOnlyAfterCheckin() throws RepositoryException {
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        node.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        if (!node.isCheckedOut()) {
            vMgr.checkout(node.getPath());
        }
        superuser.refresh(false);
        node.setProperty(propertyName1, "foo");
        superuser.save();
        vMgr.checkin(node.getPath());
        try {
            node.setProperty(propertyName1, "bar");
            fail("setProperty() must fail on a checked-in node");
        } catch (VersionException e) {
            // expected
        }
        try {
            node.setProperty(propertyName1, (String)null);
            fail("setProperty(..., null) must fail on a checked-in node");
        } catch (VersionException e) {
            // expected
        }
        try {
            Property prop = node.getProperty(propertyName1);
            assertNotNull(prop);
            prop.setValue("bar");
            fail("Property.setValue() must fail on a checked-in node");
        } catch (VersionException e) {
            // expected
        }
        try {
            Property prop = node.getProperty(propertyName1);
            assertNotNull(prop);
            prop.remove();
            fail("Property.remove() must fail on a checked-in node");
        } catch (VersionException e) {
            // expected
        }
    }

    public void testReferenceableChild() throws RepositoryException {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        Node child = node.addNode(nodeName2, ntUnstructured);
        child.addMixin(mixReferenceable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        vMgr.checkin(node.getPath());
    }

    /**
     * Test from Jackrabbit: JCR-3635 (OAK-940)
     * <p>
     * Tests the case when a node already has a manual set
     * JcrConstants.JCR_FROZENUUID property and is versioned. The manual set
     * frozenUuid will overwrite the one that is automatically assigned by the
     * VersionManager, which should not happen
     */
    public void testCopyFrozenUuidProperty() throws Exception {
        Node firstNode = testRootNode.addNode(nodeName1);
        firstNode.setPrimaryType(JcrConstants.NT_UNSTRUCTURED);
        firstNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        firstNode.getSession().save();

        // create version for the node
        Version firstNodeVersion = firstNode.checkin();
        firstNode.checkout();

        Node secondNode = testRootNode.addNode(nodeName2);
        secondNode.setPrimaryType(JcrConstants.NT_UNSTRUCTURED);
        secondNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        Property firstNodeVersionFrozenUuid = firstNodeVersion.getFrozenNode().getProperty(JcrConstants.JCR_FROZENUUID);
        secondNode.setProperty(JcrConstants.JCR_FROZENUUID, firstNodeVersionFrozenUuid.getValue());
        secondNode.getSession().save();

        // create version of the second node
        Version secondNodeVersion = secondNode.checkin();
        secondNode.checkout();

        // frozenUuid from the second node version node should not be the same as the one from the first node version
        Property secondBodeVersionFrozenUuid = secondNodeVersion.getFrozenNode().getProperty(JcrConstants.JCR_FROZENUUID);
        assertFalse(JcrConstants.JCR_FROZENUUID + " should not be the same for two different versions of different nodes! ",
                secondBodeVersionFrozenUuid.getValue().equals(firstNodeVersionFrozenUuid.getValue()));
    }

    public void testCheckoutWithPendingChanges() throws Exception {
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        node.addMixin(mixVersionable);
        superuser.save();
        node.checkin();
        Node newNode = testRootNode.addNode(nodeName2, testNodeType);
        assertTrue(newNode.isNew());
        node.checkout();
        assertTrue(node.isCheckedOut());
        assertTrue(newNode.isNew());
    }

    // OAK-1272
    public void testRemoveAndCreateSameVersionableChildNode() throws Exception {
        // create parent
        Node parentNode = testRootNode.addNode("parent");
        parentNode.setPrimaryType(ntUnstructured);
        parentNode.setProperty("name", "sample");
        // create a versionable child
        Node createdNode = parentNode.addNode("versionablechild", "nt:unstructured");
        createdNode.addMixin(mixVersionable);
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        vm.checkin(testRootNode.getPath()+"/parent/versionablechild");

        // delete and create exact same node
        Node parent = testRootNode.getNode("parent");

        // remove children
        NodeIterator nodes = parent.getNodes();
        while (nodes.hasNext()) {
            Node childNode = nodes.nextNode();
            childNode.remove();
        }

        // create again versionable child node
        Node recreatedNode = parent.addNode("versionablechild", ntUnstructured);
        recreatedNode.addMixin(mixVersionable);
        superuser.save();
    }

    // Oak-1272
    public void testRecreateVersionableNodeWithChangedProperty() throws Exception {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.setProperty(propertyName1, "foo");
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        vm.checkin(node.getPath());

        // re-create node
        node.remove();
        node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.setProperty(propertyName1, "bar");

        superuser.save();
    }

    // Oak-1272
    public void testRecreateVersionableNodeWithNewProperty() throws Exception {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        vm.checkin(node.getPath());

        // re-create node
        node.remove();
        node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.setProperty(propertyName1, "bar");

        superuser.save();
    }

    // Oak-1272
    public void testRecreateVersionableNodeWithRemovedProperty() throws Exception {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.setProperty(propertyName1, "foo");
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        vm.checkin(node.getPath());

        // re-create node
        node.remove();
        node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);

        superuser.save();
    }

    // Oak-1272
    public void testRecreateVersionableNodeWithChangedChild() throws Exception {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.addNode(nodeName2, ntUnstructured).setProperty(propertyName1, "foo");
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        vm.checkin(node.getPath());

        // re-create node
        node.remove();
        node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.addNode(nodeName2, ntUnstructured).setProperty(propertyName1, "bar");

        superuser.save();
    }

    // Oak-1272
    public void testRecreateVersionableNodeWithRemovedChild() throws Exception {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.addNode(nodeName2, ntUnstructured).setProperty(propertyName1, "foo");
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        vm.checkin(node.getPath());

        // re-create node
        node.remove();
        node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);

        superuser.save();
    }

    // Oak-1272
    public void testRecreateVersionableNodeWithAddedChild() throws Exception {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        vm.checkin(node.getPath());

        // re-create node
        node.remove();
        node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        node.addNode(nodeName2, ntUnstructured).setProperty(propertyName1, "bar");

        superuser.save();
    }

    // OAK-5193
    public void testSuccessorsPredecessorsMergedOnRemove() throws Exception {
        Node node = testRootNode.addNode(nodeName1, ntUnstructured);
        node.addMixin(mixVersionable);
        superuser.save();

        VersionManager vm = superuser.getWorkspace().getVersionManager();
        VersionHistory history = vm.getVersionHistory(node.getPath());

        vm.checkpoint(node.getPath()); // 1.0
        Version v11 = vm.checkpoint(node.getPath());
        vm.checkpoint(node.getPath()); // 1.2
        vm.checkpoint(node.getPath()); // 1.3
        vm.restore(v11, true);
        vm.checkpoint(node.getPath()); // 1.1
        vm.checkpoint(node.getPath()); // 1.1.0
        assertSuccessors(history, of("1.1.0", "1.2"), "1.1");
        vm.checkpoint(node.getPath()); // 1.1.1

        history.removeVersion("1.2");
        assertSuccessors(history, of("1.1.0", "1.3"), "1.1");
    }

    private static void assertSuccessors(VersionHistory history, Set<String> expectedSuccessors, String versionName) throws RepositoryException {
        assertEquals(expectedSuccessors, getNames(history.getVersion(versionName).getSuccessors()));
    }

    private static Set<String> getNames(Version[] versions) {
        return newHashSet(transform(asList(versions), new Function<Version, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Version input) {
                try {
                    return input.getName();
                } catch (RepositoryException e) {
                    return null;
                }
            }
        }));
    }

}