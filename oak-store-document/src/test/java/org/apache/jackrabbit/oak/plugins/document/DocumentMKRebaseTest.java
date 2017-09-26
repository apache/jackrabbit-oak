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

import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@code NodeStore#rebase}
 * FIXME: this is copied from MicroKernelImplTest. Factor out.
 */
public class DocumentMKRebaseTest extends BaseDocumentMKTest {

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

        JSONObject json = parseJSONObject(mk.getNodes("/", rebased, 0, 0, -1, null));
        assertPropertyValue(json, ":childNodeCount", 1L);
        assertNotNull(json.get("a"));
        json = parseJSONObject(mk.getNodes("/", null, 0, 0, -1, null));
        assertPropertyValue(json, ":childNodeCount", 1L);
        assertNotNull(json.get("a"));
        assertEquals(trunk, mk.getHeadRevision());
        assertFalse(trunk.equals(rebased));
    }

    @Test
    public void rebaseAddNode() {
        mk.commit("", "+\"/x\":{}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "+\"/x/b\":{}", branch, null);
        mk.commit("", "+\"/x/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertChildNodeCount("/x", null, 1);
        assertNotNull(mk.getNodes("/x/a", null, 0, 0, -1, null));

        assertChildNodeCount("/x", branch, 1);
        assertNotNull(mk.getNodes("/x/b", branch, 0, 0, -1, null));

        assertChildNodeCount("/x", rebased, 2);
        assertNotNull(mk.getNodes("/x/a", rebased, 0, 0, -1, null));
        assertNotNull(mk.getNodes("/x/b", rebased, 0, 0, -1, null));
    }

    @Test
    public void rebaseRemoveNode() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "-\"/x/y\"", branch, null);
        mk.commit("", "+\"/x/a\":{}", null, null);
        String rebased = mk.rebase(branch, null);

        assertChildNodeCount("/x", null, 2);
        assertNotNull(mk.getNodes("/x/a", null, 0, 0, -1, null));
        assertNotNull(mk.getNodes("/x/y", null, 0, 0, -1, null));

        assertChildNodeCount("/x", branch, 0);

        assertChildNodeCount("/x", rebased, 1);
        assertNotNull(mk.getNodes("/x/a", rebased, 0, 0, -1, null));
    }

    @Test
    public void rebaseAddProperty() {
        mk.commit("", "+\"/x\":{\"y\":{}}", null, null);
        String branch = mk.branch(null);
        branch = mk.commit("", "^\"/x/y/p\":42", branch, null);
        mk.commit("", "^\"/x/y/q\":99", null, null);
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
        mk.commit("", "^\"/x/y/q\":99", null, null);
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
        mk.commit("", "^\"/x/y/q\":99", null, null);
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
        mk.commit("", "^\"/x/y/p\":99", null, null);
        mk.rebase(branch, null);

        String branchNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(branchNode.contains("\"p\":99"));

        String rebasedNode = mk.getNodes("/x/y", branch, 0, 0, -1, null);
        assertTrue(rebasedNode.contains("\"p\":99"));

        String trunkNode = mk.getNodes("/x/y", null, 0, 0, -1, null);
        assertTrue(trunkNode.contains("\"p\":99"));
    }

    @Test
    @Ignore("Conflict handling")
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
    @Ignore("Conflict handling")
    public void rebaseAddExistingProperty() {
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
    @Ignore("Conflict handling")
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
    @Ignore("Conflict handling")
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
    @Ignore("Conflict handling")
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
    @Ignore("Conflict handling")
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
    @Ignore("Conflict handling")
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
    @Ignore("Conflict handling")
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
    @Ignore("Conflict handling")
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
        mk.commit("", "^\"/x/p\":42", null, null);
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

    @Test
    public void rebaseMultiple() {
        String branch = mk.branch(null);
        branch = mk.commit("/", "+\"x\":{}", branch, null);
        String head = mk.commit("/", "+\"y\":{}", null, null);
        branch = mk.rebase(branch, head);
        assertEquals("repeated rebase with same head must not create new branch revision",
                branch, mk.rebase(branch, head));
    }

}