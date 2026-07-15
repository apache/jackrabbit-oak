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
package org.apache.jackrabbit.oak.jcr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.api.util.Text;

/**
 * Some very special reference tests also including references into the version store.
 */
public class ReferencesTest extends AbstractJCRTest {

    public void testSimpleReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.setProperty("myref", ref);
        superuser.save();

        assertEquals("ref", ref.getPath(), n.getProperty("myref").getNode().getPath());
        checkReferences("refs", ref.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref.getWeakReferences());
    }

    public void testSimpleWeakReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.setProperty("myref", superuser.getValueFactory().createValue(ref, true));
        superuser.save();

        assertEquals("ref", ref.getPath(), n.getProperty("myref").getNode().getPath());
        checkReferences("refs", ref.getReferences());
        checkReferences("refs", ref.getWeakReferences(), n.getPath() + "/myref");
    }

    public void testMultipleMultiReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        Value weak = superuser.getValueFactory().createValue(ref, true);
        n.setProperty("ref1", new Value[]{weak, weak});
        n.setProperty("ref2", new Value[]{weak, weak});

        Value hard = superuser.getValueFactory().createValue(ref, false);
        n.setProperty("ref3", new Value[]{hard, hard});
        n.setProperty("ref4", new Value[]{hard, hard});
        superuser.save();

        checkReferences("refs", ref.getWeakReferences(), n.getPath() + "/ref1", n.getPath() + "/ref2");
        checkReferences("refs", ref.getReferences(), n.getPath() + "/ref3", n.getPath() + "/ref4");
    }

    // OAK-1242
    public void testWeakReferencesAddLater() throws Exception {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();
        String path = ref.getPath();
        String id = ref.getIdentifier();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        superuser.exportSystemView(path, out, false, false);
        ref.remove();
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.setProperty("myref", id, PropertyType.WEAKREFERENCE);
        superuser.save();

        // deref must fail
        try {
            n.getProperty("myref").getNode();
            fail("deref of not-existing reference target must fail.");
        } catch (RepositoryException e) {
            // ignore
        }

        // recreate node by importing it
        superuser.importXML(
                Text.getRelativeParent(path, 1),
                new ByteArrayInputStream(out.toByteArray()),
                ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        superuser.save();

        // test succeeds if reference property is re-written, too
        // n.setProperty("myref", "foo");
        // superuser.save();
        // n.setProperty("myref", id, PropertyType.WEAKREFERENCE);
        //superuser.save();

        assertEquals("ref", path, n.getProperty("myref").getNode().getPath());
        checkReferences("refs", ref.getWeakReferences(), n.getPath() + "/myref");
    }

    // OAK-1194 Missing properties in Node.getReferences()
    public void testMultipleReferencesOnSameNode() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.setProperty("myref0", ref);
        n.setProperty("myref1", ref);
        superuser.save();

        assertEquals("ref", ref.getPath(), n.getProperty("myref0").getNode().getPath());
        assertEquals("ref", ref.getPath(), n.getProperty("myref1").getNode().getPath());

        checkReferences("refs", ref.getReferences(), n.getPath() + "/myref0", n.getPath() + "/myref1");
    }

    public void testMultipleReferencesOnSameNode1() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.setProperty("myref0", ref);
        n.setProperty("myref1", ref);
        superuser.save();

        assertEquals("ref", ref.getPath(), n.getProperty("myref0").getNode().getPath());
        assertEquals("ref", ref.getPath(), n.getProperty("myref1").getNode().getPath());

        checkReferences("refs", ref.getReferences("myref0"), n.getPath() + "/myref0");
        checkReferences("refs", ref.getReferences("myref1"), n.getPath() + "/myref1");
    }

    public void testMultipleReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n0 = testRootNode.addNode(nodeName1, testNodeType);
        n0.setProperty("myref", ref);
        Node n1 = testRootNode.addNode(nodeName3, testNodeType);
        n1.setProperty("myref", ref);
        superuser.save();

        checkReferences("refs", ref.getReferences(), n0.getPath() + "/myref", n1.getPath() + "/myref");
    }

    public void testMultipleReferences1() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n0 = testRootNode.addNode(nodeName1, testNodeType);
        n0.setProperty("myref0", ref);
        Node n1 = testRootNode.addNode(nodeName3, testNodeType);
        n1.setProperty("myref1", ref);
        superuser.save();

        checkReferences("refs", ref.getReferences("myref0"), n0.getPath() + "/myref0");
        checkReferences("refs", ref.getReferences("myref1"), n1.getPath() + "/myref1");
    }

    public void testMixedReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n0 = testRootNode.addNode(nodeName1, testNodeType);
        n0.setProperty("strong_reference", ref);
        Node n1 = testRootNode.addNode(nodeName3, testNodeType);
        n1.setProperty("weak_reference", superuser.getValueFactory().createValue(ref, true));
        superuser.save();

        checkReferences("refs", ref.getReferences(), n0.getPath() + "/strong_reference");
        checkReferences("refs", ref.getWeakReferences(), n1.getPath() + "/weak_reference");
    }

    public void testMixedReferencesOnSameNode() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n0 = testRootNode.addNode(nodeName1, testNodeType);
        n0.setProperty("strong_reference", ref);
        n0.setProperty("weak_reference", superuser.getValueFactory().createValue(ref, true));
        superuser.save();

        checkReferences("refs", ref.getReferences(), n0.getPath() + "/strong_reference");
        checkReferences("refs", ref.getWeakReferences(), n0.getPath() + "/weak_reference");
    }

    public void testMixedReferencesOnSameNode3() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n0 = testRootNode.addNode(nodeName1, testNodeType);
        n0.setProperty("strong_reference", ref);
        n0.setProperty("mv_strong_reference", new Value[]{superuser.getValueFactory().createValue(ref, false)});
        n0.setProperty("weak_reference", superuser.getValueFactory().createValue(ref, true));
        n0.setProperty("mv_weak_reference", new Value[]{superuser.getValueFactory().createValue(ref, true)});
        superuser.save();

        checkReferences("refs", ref.getReferences(), n0.getPath() + "/strong_reference", n0.getPath() + "/mv_strong_reference");
        checkReferences("refs", ref.getWeakReferences(), n0.getPath() + "/weak_reference", n0.getPath() + "/mv_weak_reference");
    }

    // OAK-1195 Unable to move referenced mode
    public void testMovedReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.setProperty("myref", ref);
        superuser.save();

        String newPath = testRootNode.getPath() + "/" + nodeName3;
        superuser.move(ref.getPath(), newPath);
        superuser.save();
        ref = superuser.getNode(newPath);
        assertEquals("ref", ref.getPath(), n.getProperty("myref").getNode().getPath());
        checkReferences("refs", ref.getReferences(), n.getPath() + "/myref");
    }

    public void testMVReferences() throws RepositoryException {
        Node ref0 = testRootNode.addNode(nodeName2, testNodeType);
        ref0.addMixin(mixReferenceable);
        Node ref1 = testRootNode.addNode(nodeName3, testNodeType);
        ref1.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        Value[] vs = new Value[]{
                superuser.getValueFactory().createValue(ref0),
                superuser.getValueFactory().createValue(ref1)
        };
        n.setProperty("myref", vs);
        superuser.save();

        assertEquals("ref0", ref0.getIdentifier(), n.getProperty("myref").getValues()[0].getString());
        assertEquals("ref1", ref1.getIdentifier(), n.getProperty("myref").getValues()[1].getString());
        checkReferences("refs", ref0.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref1.getReferences(), n.getPath() + "/myref");
    }

    // OAK-1257
    public void testMVAddReferences() throws RepositoryException {
        Node ref0 = testRootNode.addNode(nodeName2, testNodeType);
        ref0.addMixin(mixReferenceable);
        Node ref1 = testRootNode.addNode(nodeName3, testNodeType);
        ref1.addMixin(mixReferenceable);
        Node ref2 = testRootNode.addNode(nodeName4, testNodeType);
        ref2.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        Value[] vs = new Value[]{
                superuser.getValueFactory().createValue(ref0),
                superuser.getValueFactory().createValue(ref1)
        };
        n.setProperty("myref", vs);
        superuser.save();

        assertEquals("ref0", ref0.getIdentifier(), n.getProperty("myref").getValues()[0].getString());
        assertEquals("ref1", ref1.getIdentifier(), n.getProperty("myref").getValues()[1].getString());
        checkReferences("refs", ref0.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref1.getReferences(), n.getPath() + "/myref");

        vs = new Value[]{
                superuser.getValueFactory().createValue(ref0),
                superuser.getValueFactory().createValue(ref1),
                superuser.getValueFactory().createValue(ref2)
        };
        n.setProperty("myref", vs);
        superuser.save();

        assertEquals("ref0", ref0.getIdentifier(), n.getProperty("myref").getValues()[0].getString());
        assertEquals("ref1", ref1.getIdentifier(), n.getProperty("myref").getValues()[1].getString());
        assertEquals("ref2", ref2.getIdentifier(), n.getProperty("myref").getValues()[2].getString());
        checkReferences("refs", ref0.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref1.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref2.getReferences(), n.getPath() + "/myref");
    }

    // OAK-1300
    public void testMVRemoveReferences() throws RepositoryException {
        Node ref0 = testRootNode.addNode(nodeName2, testNodeType);
        ref0.addMixin(mixReferenceable);
        Node ref1 = testRootNode.addNode(nodeName3, testNodeType);
        ref1.addMixin(mixReferenceable);
        Node ref2 = testRootNode.addNode(nodeName4, testNodeType);
        ref2.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        Value[] vs = new Value[]{
                superuser.getValueFactory().createValue(ref0),
                superuser.getValueFactory().createValue(ref1),
                superuser.getValueFactory().createValue(ref2)
        };
        n.setProperty("myref", vs);
        superuser.save();

        assertEquals("ref0", ref0.getIdentifier(), n.getProperty("myref").getValues()[0].getString());
        assertEquals("ref1", ref1.getIdentifier(), n.getProperty("myref").getValues()[1].getString());
        assertEquals("ref2", ref2.getIdentifier(), n.getProperty("myref").getValues()[2].getString());
        checkReferences("refs", ref0.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref1.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref2.getReferences(), n.getPath() + "/myref");

        vs = new Value[]{
                superuser.getValueFactory().createValue(ref1),
                superuser.getValueFactory().createValue(ref2)
        };
        n.setProperty("myref", vs);
        superuser.save();

        assertEquals("ref1", ref1.getIdentifier(), n.getProperty("myref").getValues()[0].getString());
        assertEquals("ref2", ref2.getIdentifier(), n.getProperty("myref").getValues()[1].getString());
        checkReferences("refs", ref0.getReferences());
        checkReferences("refs", ref1.getReferences(), n.getPath() + "/myref");
        checkReferences("refs", ref2.getReferences(), n.getPath() + "/myref");
    }

    public void testVersionReferencesVH() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();

        String p = n.getPath();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        VersionHistory vh = vMgr.getVersionHistory(p);

        // check if versionable node has references to root version
        assertEquals("Version History", vh.getIdentifier(), n.getProperty(Property.JCR_VERSION_HISTORY).getString());

        checkReferences("Version History", vh.getReferences(), p + "/jcr:versionHistory");
    }

    public void testVersionReferencesV0() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();

        String p = n.getPath();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        Version v0 = vMgr.getVersionHistory(p).getRootVersion();

        // check if versionable node has references to root version
        assertEquals("Root Version", v0.getIdentifier(), n.getProperty(Property.JCR_BASE_VERSION).getString());
        assertEquals("Root Version", v0.getIdentifier(), n.getProperty(Property.JCR_PREDECESSORS).getValues()[0].getString());

        checkReferences("Root Version", v0.getReferences(),
                p + "/jcr:baseVersion",
                p + "/jcr:predecessors"
        );
    }

    public void testVersionReferencesV1() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();

        String p = n.getPath();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        Version v1 = vMgr.checkpoint(p);

        // check if versionable node has references to v1.0
        assertEquals("v1.0", v1.getIdentifier(), n.getProperty(Property.JCR_BASE_VERSION).getString());
        assertEquals("v1.0", v1.getIdentifier(), n.getProperty(Property.JCR_PREDECESSORS).getValues()[0].getString());

        checkReferences("v1.0", v1.getReferences(),
                p + "/jcr:baseVersion",
                p + "/jcr:predecessors"
        );
    }

    // OAK-1196 - Node.getReferences() should not show references in frozen nodes
    public void testVersionedReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        n.setProperty("myref", ref);
        superuser.save();

        String p = n.getPath();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        Version v1 = vMgr.checkpoint(p);
        Node frozen = v1.getFrozenNode();

        assertEquals("ref", ref.getPath(), frozen.getProperty("myref").getNode().getPath());

        checkReferences("ref in version store", ref.getReferences(), n.getPath() + "/myref");

        // also test what happens if node is removed
        n.remove();
        ref.remove();
        superuser.save();

        try {
            frozen.getProperty("myref").getNode();
            fail("removed reference should not be accessible");
        } catch (ItemNotFoundException e) {
            // ok
        }
    }

    public void testMovedVersionedReferences() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName2, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        n.setProperty("myref", ref);
        superuser.save();

        String p = n.getPath();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        Version v1 = vMgr.checkpoint(p);

        String newPath = testRootNode.getPath() + "/" + nodeName3;
        superuser.move(ref.getPath(), newPath);
        superuser.save();
        ref = superuser.getNode(newPath);

        Node frozen = v1.getFrozenNode();
        assertEquals("ref", ref.getPath(), frozen.getProperty("myref").getNode().getPath());
        checkReferences("ref in version store", ref.getReferences(), n.getPath() + "/myref");
    }

    public void testRemoveReferenced1() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName1, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n1 = testRootNode.addNode(nodeName2, testNodeType);
        n1.setProperty("ref", ref);
        assertEquals(PropertyType.REFERENCE, n1.getProperty("ref").getType());
        superuser.save();

        ref.remove();
        n1.remove();
        superuser.save();
    }

    public void testRemoveReferenced2() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName1, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n1 = testRootNode.addNode(nodeName2, testNodeType);
        n1.setProperty("ref", ref);
        assertEquals(PropertyType.REFERENCE, n1.getProperty("ref").getType());
        Node n2 = testRootNode.addNode(nodeName3, testNodeType);
        n2.setProperty("ref", ref);
        assertEquals(PropertyType.REFERENCE, n2.getProperty("ref").getType());
        superuser.save();

        ref.remove();
        n1.remove();
        try {
            superuser.save();
            fail("must fail with ReferentialIntegrityException");
        } catch (ReferentialIntegrityException e) {
            // expected
        }
    }

    public void testRemoveReferenced3() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName1, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n1 = testRootNode.addNode(nodeName2, testNodeType);
        n1.setProperty("ref", ref);
        assertEquals(PropertyType.REFERENCE, n1.getProperty("ref").getType());
        superuser.save();

        Node n2 = testRootNode.addNode(nodeName3, testNodeType);
        n2.setProperty("ref", ref);
        assertEquals(PropertyType.REFERENCE, n2.getProperty("ref").getType());
        ref.remove();
        n1.remove();
        try {
            superuser.save();
            fail("must fail with ReferentialIntegrityException");
        } catch (ReferentialIntegrityException e) {
            // expected
        }
    }

    public void testRecreateWithDifferentUUID() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName1, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();
        String uuid = ref.getIdentifier();

        Node n1 = testRootNode.addNode(nodeName2, testNodeType);
        n1.setProperty("ref", ref);
        assertEquals(PropertyType.REFERENCE, n1.getProperty("ref").getType());
        superuser.save();

        // recreate
        ref.remove();
        ref = testRootNode.addNode(nodeName1, testNodeType);
        ref.addMixin(mixReferenceable);
        assertFalse(uuid.equals(ref.getIdentifier()));
        try {
            superuser.save();
            fail("must fail with ReferentialIntegrityException");
        } catch (ReferentialIntegrityException e) {
            // expected
        }
    }

    public void testRecreateNonReferenceable() throws RepositoryException {
        Node ref = testRootNode.addNode(nodeName1, testNodeType);
        ref.addMixin(mixReferenceable);
        superuser.save();

        Node n1 = testRootNode.addNode(nodeName2, testNodeType);
        n1.setProperty("ref", ref);
        assertEquals(PropertyType.REFERENCE, n1.getProperty("ref").getType());
        superuser.save();

        // recreate
        ref.remove();
        testRootNode.addNode(nodeName1, testNodeType);
        try {
            superuser.save();
            fail("must fail with ReferentialIntegrityException");
        } catch (ReferentialIntegrityException e) {
            // expected
        }
    }

    private static void checkReferences(String msg, PropertyIterator refs, String ... expected) throws RepositoryException {
        List<String> paths = new LinkedList<String>();
        while (refs.hasNext()) {
            paths.add(refs.nextProperty().getPath());
        }
        checkEquals(msg, paths, expected);
    }

    private static void checkEquals(String msg, List<String> result, String ... expected) {
        List<String> exp = Arrays.asList(expected);
        Collections.sort(result);
        Collections.sort(exp);
        assertEquals(msg, exp.toString(), result.toString());
    }
}
