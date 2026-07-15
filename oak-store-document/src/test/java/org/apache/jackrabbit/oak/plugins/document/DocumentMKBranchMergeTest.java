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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@code NodeStore#branch}
 */
public class DocumentMKBranchMergeTest extends BaseDocumentMKTest {

    @Test
    public void oneBranchAddedChildren1() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");
        assertNodesNotExist(null, "/branch1", "/branch1/child1");

        addNodes(null, "/trunk/child2");
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev, "/trunk/child2");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child2", "/branch1", "/branch1/child1");
    }

    @Test
    public void oneBranchAddedChildren2() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/trunk/child1/child2");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev, "/trunk/child1/child2");
        assertNodesNotExist(null, "/trunk/child1/child2");

        addNodes(null, "/trunk/child3");
        assertNodesExist(null, "/trunk/child3");
        assertNodesNotExist(branchRev, "/trunk/child3");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child3");
    }

    @Test
    public void oneBranchAddedChildren3() {
        addNodes(null, "/root", "/root/child1");
        assertNodesExist(null, "/root", "/root/child1");

        String branchRev = mk.branch(null);
        // System.out.println("branchRev: " + branchRev);

        addNodes(null, "/root/child2");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2");
        assertNodesExist(branchRev, "/root", "/root/child1");
        assertNodesNotExist(branchRev, "/root/child2");

        branchRev = addNodes(branchRev, "/root/child1/child3", "/root/child4");
        assertNodesExist(branchRev, "/root", "/root/child1", "/root/child1/child3", "/root/child4");
        assertNodesNotExist(branchRev, "/root/child2");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2");
        assertNodesNotExist(null, "/root/child1/child3", "/root/child4");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2",
                "/root/child1/child3", "/root/child4");
    }

    @Test
    public void oneBranchRemovedChildren() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = removeNodes(branchRev, "/trunk/child1");
        assertNodesExist(branchRev, "/trunk");
        assertNodesNotExist(branchRev, "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk");
        assertNodesNotExist(null, "/trunk/child1");
    }

    @Test
    public void oneBranchRemovedRoot() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = removeNodes(branchRev, "/trunk");
        assertNodesNotExist(branchRev, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        mk.merge(branchRev, "");
        assertNodesNotExist(null, "/trunk", "/trunk/child1");
    }

    /**
     * This is a test to make sure properties are properly escaped in merge.
     */
    @Test
    public void oneBranchAddPropertyRoot() {
        String branchRev = mk.branch(null);

        branchRev = setProp(branchRev, "/jcr:primaryType", "nam:rep:root");
        assertPropExists(branchRev, "/", "jcr:primaryType");

        branchRev = mk.merge(branchRev, "");
        assertPropExists(branchRev, "/", "jcr:primaryType");

        String mergedNode = mk.getNodes("/", branchRev, 0, 0, -1, null);
        String expectedNode = "{\"jcr:primaryType\":\"nam:rep:root\",\":childNodeCount\":0}";
        assertEquals("Wrong property value after merge", expectedNode, mergedNode);
    }

    @Test
    public void oneBranchChangedProperties() {
        addNodes(null, "/trunk", "/trunk/child1");
        setProp(null, "/trunk/child1/prop1", "value1");
        setProp(null, "/trunk/child1/prop2", "value2");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");

        String branchRev = mk.branch(null);

        branchRev = setProp(branchRev, "/trunk/child1/prop1", "value1a");
        branchRev = setProp(branchRev, "/trunk/child1/prop2", null);
        branchRev = setProp(branchRev, "/trunk/child1/prop3", "value3");
        assertPropValue(branchRev, "/trunk/child1", "prop1", "value1a");
        assertPropNotExists(branchRev, "/trunk/child1", "prop2");
        assertPropValue(branchRev, "/trunk/child1", "prop3", "value3");
        assertPropValue(null, "/trunk/child1", "prop1", "value1");
        assertPropExists(null, "/trunk/child1", "prop2");
        assertPropNotExists(null, "/trunk/child1", "prop3");

        mk.merge(branchRev, "");
        assertPropValue(null, "/trunk/child1", "prop1", "value1a");
        assertPropNotExists(null, "/trunk/child1", "prop2");
        assertPropValue(null, "/trunk/child1", "prop3", "value3");
    }

    @Test
    public void oneBranchAddedSubChildren() {
        addNodes(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
        assertNodesNotExist(null, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");

        addNodes(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");
        assertNodesExist(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");
        assertNodesNotExist(branchRev, "/trunk/child1/child2/child3/child4", "/trunk/child5");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3", "/trunk/child1/child2/child3/child4");
        assertNodesExist(null, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
    }

    @Test
    public void oneBranchAddedChildrenAndAddedProperties() {
        addNodes(null, "/trunk", "/trunk/child1");
        setProp(null, "/trunk/child1/prop1", "value1");
        setProp(null, "/trunk/child1/prop2", "value2");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1");
        branchRev = setProp(branchRev, "/branch1/child1/prop1", "value1");
        branchRev = setProp(branchRev, "/branch1/child1/prop2", "value2");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertPropExists(branchRev, "/trunk/child1", "prop1");
        assertPropExists(branchRev, "/trunk/child1", "prop2");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");
        assertPropExists(branchRev, "/branch1/child1", "prop1");
        assertPropExists(branchRev, "/branch1/child1", "prop2");
        assertNodesNotExist(null, "/branch1", "/branch1/child1");
        assertPropNotExists(null, "/branch1/child1", "prop1");
        assertPropNotExists(null, "/branch1/child1", "prop2");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");
        assertNodesExist(null, "/branch1", "/branch1/child1");
        assertPropExists(null, "/branch1/child1", "prop1");
        assertPropExists(null, "/branch1/child1", "prop2");
    }

    @Test
    public void twoBranchesAddedChildren1() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev1 = mk.branch(null);
        String branchRev2 = mk.branch(null);

        branchRev1 = addNodes(branchRev1, "/branch1", "/branch1/child1");
        branchRev2 = addNodes(branchRev2, "/branch2", "/branch2/child2");
        assertNodesExist(branchRev1, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev2, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev1, "/branch1/child1");
        assertNodesNotExist(branchRev1, "/branch2/child2");
        assertNodesExist(branchRev2, "/branch2/child2");
        assertNodesNotExist(branchRev2, "/branch1/child1");
        assertNodesNotExist(null, "/branch1/child1", "/branch2/child2");

        addNodes(null, "/trunk/child2");
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev1, "/trunk/child2");
        assertNodesNotExist(branchRev2, "/trunk/child2");

        mk.merge(branchRev1, "");
        assertNodesExist(null, "/trunk", "/branch1", "/branch1/child1");
        assertNodesNotExist(null, "/branch2", "/branch2/child2");

        mk.merge(branchRev2, "");
        assertNodesExist(null, "/trunk", "/branch1", "/branch1/child1", "/branch2", "/branch2/child2");
    }

    @Test
    @Ignore
    public void oneBranchAddedChildrenWithConflict() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = removeNodes(branchRev, "/trunk/child1");
        assertNodesExist(branchRev, "/trunk");
        assertNodesNotExist(branchRev, "/trunk/child1");

        addNodes(null, "/trunk/child1/child2");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk");
        assertNodesNotExist(null, "/trunk/child1", "/trunk/child1/child2");
    }

    @Test
    public void propertyConflictWithMergedBranch() {
        addNodes(null, "/trunk");
        String rev = setProp(null, "/trunk/prop1", "value1");
        assertPropExists(null, "/trunk", "prop1");

        String branchRev = mk.branch(null);

        branchRev = setProp(branchRev, "/trunk/prop1", "value1a");
        assertPropValue(branchRev, "/trunk", "prop1", "value1a");

        mk.merge(branchRev, "");
        assertPropValue(null, "/trunk", "prop1", "value1a");
        try {
            setProp(rev, "/trunk/prop1", "value1b");
            fail("Expected: Concurrent modification exception");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void oneBranchChangedPropertiesWithConflict() {
        addNodes(null, "/trunk");
        setProp(null, "/trunk/prop1", "value1");
        assertPropExists(null, "/trunk", "prop1");

        String branchRev = mk.branch(null);

        branchRev = setProp(branchRev, "/trunk/prop1", "value1a");
        assertPropValue(branchRev, "/trunk", "prop1", "value1a");

        setProp(null, "/trunk/prop1", "value1b");
        try {
            mk.merge(branchRev, "");
            fail("Expected: Concurrent modification exception");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void twoBranchChangedPropertiesWithConflict() {
        addNodes(null, "/trunk");
        setProp(null, "/trunk/prop1", "value1");
        setProp(null, "/trunk/prop2", "value1");
        assertPropExists(null, "/trunk", "prop1");
        assertPropExists(null, "/trunk", "prop2");

        String branchRev1 = mk.branch(null);
        branchRev1 = setProp(branchRev1, "/trunk/prop1", "value1-b1");
        assertPropValue(branchRev1, "/trunk", "prop1", "value1-b1");

        String branchRev2 = mk.branch(null);
        branchRev2 = setProp(branchRev2, "/trunk/prop2", "value1-b2");
        assertPropValue(branchRev2, "/trunk", "prop2", "value1-b2");

        // creates a conflict for both branches
        mk.commit("/", "^\"trunk/prop1\":\"value1-modified\"" +
                "^\"trunk/prop2\":\"value1-modified\"", null, null);
        try {
            mk.merge(branchRev1, "");
            fail("Expected: Concurrent modification exception");
        } catch (Exception expected) {
            // expected
        }

        try {
            mk.merge(branchRev2, "");
            fail("Expected: Concurrent modification exception");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void addExistingRootInBranch() {
        addNodes(null, "/root");
        assertNodesExist(null, "/root");

        String branchRev = mk.branch(null);
        try {
            addNodes(branchRev, "/root");
            fail("Should not be able to add the same root node twice");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void addExistingChildInBranch() {
        addNodes(null, "/root", "/root/child1");
        assertNodesExist(null, "/root", "/root/child1");

        String branchRev = mk.branch(null);
        branchRev = addNodes(branchRev, "/root/child2");
        assertNodesExist(branchRev, "/root/child1", "/root/child2");

        try {
            addNodes(branchRev, "/root/child1");
            fail("Should not be able to add the same root node twice");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void trunkMergeNotAllowed() {
        String rev = mk.commit("", "+\"/child1\":{}", null, "");
        try {
            mk.merge(rev, "");
            fail("Exception expected");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    @Ignore
    public void movesInBranch() {
        String rev = mk.commit("/", "+\"a\":{\"b\":{}}", null, null);
        String branchRev = mk.branch(rev);
        branchRev = mk.commit("/", ">\"a\":\"x\"^\"x/b/p\":1>\"x\":\"a\"", branchRev, null);
        rev = mk.merge(branchRev, null);
        assertNodesExist(rev, "/a", "/a/b");
        assertPropExists(rev, "/a/b", "p");
    }

    @Test
    public void concurrentNonConflictingMerges() throws Exception {
        int numThreads = 10;
        mk.commit("/", "+\"test\":{}", null, null);
        List<Thread> workers = new ArrayList<Thread>();
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        for (int i = 0; i < numThreads; i++) {
            final String path = "/test/t" + i;
            mk.commit("", "+\"" + path + "\":{}", null, null);
            workers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 50; i++) {
                            String branchRev = mk.branch(null);
                            branchRev = mk.commit(path, "+\"node" + i + "\":{}", branchRev, null);
                            mk.merge(branchRev, null);
                        }
                    } catch (DocumentStoreException e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }

    //--------------------------< internal >------------------------------------

    private String setProp(String rev, String prop, Object value) {
        value = value == null? null : "\"" + value + "\"";
        return mk.commit("", "^\"" + prop + "\" : " + value, rev, "");
    }
}