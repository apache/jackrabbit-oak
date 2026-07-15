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

import org.junit.Test;

/**
 * Tests if commits to branches and trunk are properly isolated and repository
 * state on a given revision is stable.
 */
public class IsolationTest extends BaseDocumentMKTest {

    @Test
    public void phantomReadOnBranch() {
        String base = mk.commit("/", "+\"test\":{}", null, null);
        String branchRev1 = mk.branch(base);
        String branchRev2 = mk.branch(base);
        branchRev1 = mk.commit("/test", "+\"node1\":{}", branchRev1, null);
        branchRev2 = mk.commit("/test", "+\"node2\":{}", branchRev2, null);
        String r = mk.commit("/test", "+\"node3\":{}", null, null);
        // branchRev1 must not see node3 at this point
        assertNodesNotExist(branchRev1, "/test/node3");

        // this will make node3 visible to branchRev1
        branchRev1 = mk.rebase(branchRev1, r);
        assertNodesExist(branchRev1, "/test/node1", "/test/node3");
        assertNodesNotExist(branchRev1, "/test/node2");

        // merging second branch must not have an effect on
        // rebased first branch
        mk.merge(branchRev2, null);
        assertNodesExist(branchRev1, "/test/node1", "/test/node3");
        assertNodesNotExist(branchRev1, "/test/node2");
    }

    @Test
    public void phantomReadOnTrunk() {
        String base = mk.commit("/", "+\"test\":{}", null, null);
        String branchRev1 = mk.branch(base);
        branchRev1 = mk.commit("/test", "+\"node1\":{}", branchRev1, null);
        String rev = mk.commit("/test", "+\"node2\":{}", null, null);
        // rev must not see node1
        assertNodesNotExist(rev, "/test/node1");

        mk.merge(branchRev1, null);

        // rev still must not see node1
        assertNodesNotExist(rev, "/test/node1");
    }
}
