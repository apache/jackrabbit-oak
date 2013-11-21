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
package org.apache.jackrabbit.oak.plugins.mongomk;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests MongoMKs implementation of MicroKernel.reset(String, String).
 */
public class MongoMKResetTest extends BaseMongoMKTest {

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
}
