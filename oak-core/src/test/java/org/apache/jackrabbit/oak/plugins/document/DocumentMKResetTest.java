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

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests DocumentMKs implementation of MicroKernel.reset(String, String).
 */
public class DocumentMKResetTest extends BaseDocumentMKTest {

    @Test
    public void resetToCurrentBranchHead() {
        String rev = mk.branch(null);
        rev = addNodes(rev, "/foo");
        String reset = mk.reset(rev, rev);
        assertTrue(mk.diff(rev, reset, "/", 0).length() == 0);
    }

    @Test
    public void resetTrunk() {
        String rev = addNodes(null, "/foo");
        try {
            mk.reset(rev, rev);
            fail("MicroKernelException expected");
        } catch (MicroKernelException expected) {}
    }

    @Test
    public void resetNonAncestor() {
        String rev = mk.getHeadRevision();
        addNodes(null, "/foo");
        String branch = mk.branch(null);
        branch = addNodes(branch, "/bar");
        try {
            mk.reset(branch, rev);
            fail("MicroKernelException expected");
        } catch (MicroKernelException expected) {}
    }

    @Test
    public void resetBranch() {
        String branch = mk.branch(null);
        branch = addNodes(branch, "/foo");
        String head = addNodes(branch, "/bar");
        assertNodesExist(head, "/bar");
        head = mk.reset(head, branch);
        assertNodesNotExist(head, "/bar");
    }

    @Test
    public void resetConflictAddExistingNode() {
        String b0 = mk.branch(null);
        addNodes(null, "/foo");
        String b1 = addNodes(b0, "/bar");
        String b2 = addNodes(b1, "/foo");
        try {
            mk.merge(b2, null);
            fail("merge with conflict must fail");
        } catch (MicroKernelException e) {
            // expected
        }
        String b3 = mk.reset(b2, b1);
        String rev = mk.merge(b3, null);

        assertNodesExist(rev, "/foo", "/bar");
    }

    @Test
    public void resetConflictRemoveRemovedNode() {
        String rev = addNodes(null, "/foo", "/bar");
        String b0 = mk.branch(rev);
        removeNodes(null, "/foo");
        String b1 = removeNodes(b0, "/bar");
        String b2 = removeNodes(b1, "/foo");
        try {
            mk.merge(b2, null);
            fail("merge with conflict must fail");
        } catch (MicroKernelException e) {
            // expected
        }
        String b3 = mk.reset(b2, b1);
        rev = mk.merge(b3, null);

        assertNodesNotExist(rev, "/foo", "/bar");
    }

    @Ignore
    @Test
    public void resetConflictAddExistingProperty() {
        addNodes(null, "/foo");
        String b0 = mk.branch(null);
        mk.commit("", "^\"/foo/p1\":1", null, null);
        String b1 = mk.commit("", "^\"/foo/p2\":1", b0, null);
        String b2 = mk.commit("", "^\"/foo/p1\":1", b1, null);
        try {
            mk.merge(b2, null);
            fail("merge with conflict must fail");
        } catch (MicroKernelException e) {
            // expected
        }
        String b3 = mk.reset(b2, b1);
        String rev = mk.merge(b3, null);

        assertPropExists(rev, "/foo", "p1");
        assertPropExists(rev, "/foo", "p2");
    }
}
