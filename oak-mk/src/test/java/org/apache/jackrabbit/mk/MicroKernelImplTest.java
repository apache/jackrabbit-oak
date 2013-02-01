/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MicroKernelImplTest {
    
    private File homeDir;
    private MicroKernelImpl mk;
    
    @Before
    public void setup() throws Exception {
        homeDir = new File("target/mk");
        if (homeDir.exists()) {
            FileUtils.cleanDirectory(homeDir);
        }
        mk = new MicroKernelImpl(homeDir.getPath());
    }
    
    @After
    public void tearDown() throws Exception {
        if (mk != null) {
            mk.dispose();
        }
    }
    
    /**
     * OAK-276: potential clash of commit id's after restart.
     */
    @Test
    public void potentialClashOfCommitIds() {
        String headRev = mk.commit("/", "+\"a\" : {}", mk.getHeadRevision(), null);
        String branchRev = mk.branch(mk.getHeadRevision());
        
        mk.dispose();
        mk = new MicroKernelImpl(homeDir.getPath());
        assertEquals("Stored head should be equal", headRev, mk.getHeadRevision());

        headRev = mk.commit("/", "+\"b\" : {}", mk.getHeadRevision(), null);
        assertFalse("Commit must not have same id as branch", headRev.equals(branchRev));
    }
    
    @Test
    public void diffWithDepth() {
        String head = mk.getHeadRevision();
        String r1 = mk.commit("/", "+\"a\" : { \"l\": 1, \"x\": { \"l\": 2, \"y\": {} } }", head, null);
        String r2 = mk.commit("/", ">\"a\" : \"b\"", r1, null);
        assertEquals("+\"/b\":{\"l\":1}", mk.diff(r1, r2, "/b", 0).trim());
        assertEquals("+\"/b\":{\"l\":1,\"x\":{\"l\":2}}", mk.diff(r1, r2, "/b", 1).trim());
        assertEquals("+\"/b\":{\"l\":1,\"x\":{\"l\":2,\"y\":{}}}", mk.diff(r1, r2, "/b", -1).trim());
    }


    @Test
    public void rebaseWithoutChanges() {
        String branch = mk.branch(null);
        String rebased = mk.rebase(branch, null);
        assertEquals(branch, rebased);
    }

    @Test
    public void fastForwardRebase() {
        String branch = mk.branch(null);
        branch = mk.commit("", "+\"/a\":{}", branch, null);
        String rebased = mk.rebase(branch, null);
        assertEquals(branch, rebased);
    }

    @Test
    public void rebaseEmptyBranch() {
        String branch = mk.branch(null);
        String trunk = mk.commit("", "+\"/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertEquals("{\":childNodeCount\":1,\"a\":{}}", mk.getNodes("/", rebased, 0, 0, -1, null));
        assertEquals("{\":childNodeCount\":1,\"a\":{}}", mk.getNodes("/", null, 0, 0, -1, null));
        assertEquals(trunk, mk.getHeadRevision());
        assertFalse((trunk.equals(rebased)));
    }

    @Test
    public void rebaseAddNode() {
        mk.commit("", "+\"/x\":{}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "+\"/x/b\":{}", branch, null);
        String trunk = mk.commit("", "+\"/x/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertEquals(1, mk.getChildNodeCount("/x", null));
        assertNotNull(mk.getNodes("/x/a", null, 0, 0, -1, null));

        assertEquals(1, mk.getChildNodeCount("/x", branch));
        assertNotNull(mk.getNodes("/x/b", branch, 0, 0, -1, null));

        assertEquals(2, mk.getChildNodeCount("/x", rebased));
        assertNotNull(mk.getNodes("/x/a", rebased, 0, 0, -1, null));
        assertNotNull(mk.getNodes("/x/b", rebased, 0, 0, -1, null));
    }

    @Test
    public void rebaseRemoveNode() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "-\"/x/y\"", branch, null);
        String trunk = mk.commit("", "+\"/x/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertEquals(2, mk.getChildNodeCount("/x", null));
        assertNotNull(mk.getNodes("/x/a", null, 0, 0, -1, null));
        assertNotNull(mk.getNodes("/x/y", null, 0, 0, -1, null));

        assertEquals(0, mk.getChildNodeCount("/x", branch));

        assertEquals(1, mk.getChildNodeCount("/x", rebased));
        assertNotNull(mk.getNodes("/x/a", rebased, 0, 0, -1, null));
    }

    @Test
    public void rebaseAddProperty() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":42", branch, null);
        String trunk = mk.commit("", "^\"/x/y/q\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":42"));
        assertFalse(branchNode.contains("\"q\":99"));

        String rebasedNode = mk.getNodes("/x/y", rebased, 0, 0, -1, null);
        assertTrue(rebasedNode.contains("\"p\":42"));
        assertTrue(rebasedNode.contains("\"q\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertFalse(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":99"));
    }

    @Test
    public void rebaseRemoveProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":null", branch, null);
        String trunk = mk.commit("", "^\"/x/y/q\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertFalse(branchNode.contains("\"p\":42"));
        assertFalse(branchNode.contains("\"q\":99"));

        String rebasedNode = mk.getNodes("/x/y", rebased, 0, 0, -1, null);
        assertFalse(rebasedNode.contains("\"p\":42"));
        assertTrue(rebasedNode.contains("\"q\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":99"));
    }

    @Test
    public void rebaseChangeProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":41", branch, null);
        String trunk = mk.commit("", "^\"/x/y/q\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":41"));
        assertFalse(branchNode.contains("\"q\":99"));

        String rebasedNode = mk.getNodes("/x/y", rebased, 0, 0, -1, null);
        assertTrue(rebasedNode.contains("\"p\":41"));
        assertTrue(rebasedNode.contains("\"q\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":99"));
    }

    @Test
    public void rebaseChangePropertyWithSameValue() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":99", branch, null);
        String trunk = mk.commit("", "^\"/x/y/p\":99", null, null);
        String rebased = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":99"));

        String rebasedNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(rebasedNode.contains("\"p\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":99"));
    }

    @Test
    public void rebaseAddExistingNode() {
        mk.commit("", "+\"/x\":{}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "+\"/x/a\":{}", branch, null);
        mk.commit("", "+\"/x/a\":{\"b\":{}}", null, null);

        branch = mk.rebase(branch, null);

        assertTrue(mk.nodeExists("/x/a/b", branch));
        String conflict = mk.getNodes("/x/:conflict", branch, 100, 0, -1, null);
        assertEquals(
            "{\":childNodeCount\":1,\"addExistingNode\":{\":childNodeCount\":1,\"a\":{\":childNodeCount\":0}}}",
            conflict);
    }

    @Test
    public void rebaseAddExistingProperties() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":42 ^\"/x/y/q\":42", branch, null);
        mk.commit("", "^\"/x/y/p\":99 ^\"/x/y/q\":99", null, null);

        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":99"));
        String conflict = mk.getNodes("/x/y/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"addExistingProperty\":{\"q\":42,\"p\":42,\":childNodeCount\":0}}",
                conflict);
    }

    @Test
    public void rebaseChangeRemovedProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":99", branch, null);
        mk.commit("", "^\"/x/y/p\":null", null, null);

        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertFalse(branchNode.contains("\"p\":99"));
        String conflict = mk.getNodes("/x/y/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"changeDeletedProperty\":{\"p\":99,\":childNodeCount\":0}}",
                conflict);
    }

    @Test
    public void rebaseRemoveChangedProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":null", branch, null);
        mk.commit("", "^\"/x/y/p\":99", null, null);

        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":99"));
        String conflict = mk.getNodes("/x/y/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"deleteChangedProperty\":{\"p\":42,\":childNodeCount\":0}}",
                conflict);
    }

    @Test
    public void rebaseChangedChangedProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":41", branch, null);
        mk.commit("", "^\"/x/y/p\":99", null, null);

        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":99"));
        String conflict = mk.getNodes("/x/y/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"changeChangedProperty\":{\"p\":41,\":childNodeCount\":0}}",
                conflict);
    }

    @Test
    public void rebaseRemoveChangedNode() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "-\"/x/y\"", branch, null);
        mk.commit("", "^\"/x/y/p\":42", null, null);

        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":42"));
        String conflict = mk.getNodes("/x/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"deleteChangedNode\":{\":childNodeCount\":1,\"y\":{\":childNodeCount\":0}}}",
                conflict);
    }

    @Test
    public void rebaseChangeRemovedNode() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/p\":42", branch, null);
        mk.commit("", "-\"/x\"", null, null);

        branch = mk.rebase(branch, null);

        assertFalse(mk.nodeExists("/x", branch));
        String conflict = mk.getNodes("/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"changeDeletedNode\":{\":childNodeCount\":1,\"x\":{\"p\":42,\"" +
                ":childNodeCount\":1,\"y\":{\":childNodeCount\":0}}}}",
                conflict);
    }

    @Test
    public void rebaseRemoveRemovedProperty() {
        mk.commit("", "+\"/x\":{\"y\":{\"p\":42}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":null", branch, null);
        mk.commit("", "^\"/x/y/p\":null", null, null);

        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertFalse(branchNode.contains("\"p\":42"));
        String conflict = mk.getNodes("/x/y/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"deleteDeletedProperty\":{\"p\":42,\":childNodeCount\":0}}",
                conflict);
    }

    @Test
    public void rebaseRemoveRemovedNode() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "-\"/x/y\"", branch, null);
        mk.commit("", "-\"/x/y\"", null, null);

        branch = mk.rebase(branch, null);

        assertFalse(mk.nodeExists("/x/y", branch));
        String conflict = mk.getNodes("/x/:conflict", branch, 100, 0, -1, null);
        assertEquals(
                "{\":childNodeCount\":1,\"deleteDeletedNode\":{\":childNodeCount\":1,\"y\":{\":childNodeCount\":0}}}",
                conflict);
    }

    @Test
    public void mergeRebased() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        String trunk = mk.commit("", "^\"/x/p\":42", null, null);
        branch = mk.commit("", "^\"/x/q\":43", branch, null);
        branch = mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":42"));
        assertTrue(branchNode.contains("\"q\":43"));

        mk.merge(branch, null);
        String trunkNode = mk.getNodes("/x", branch, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":42"));
        assertTrue(trunkNode.contains("\"q\":43"));
    }

}
