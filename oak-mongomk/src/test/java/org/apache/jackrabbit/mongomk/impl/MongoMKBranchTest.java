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
package org.apache.jackrabbit.mongomk.impl;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <code>MongoMKBranchTest</code> performs a test to check if commits
 * to a branch are not visible to other branches.
 */
public class MongoMKBranchTest extends BaseMongoMicroKernelTest {

    /**
     * Creates the following revision history:
     * <pre>
     *   + rev1 (first commit with /child1)
     *   |\
     *   | + branchRev1 (branch from rev1)
     *   | + branchRev11 (branch commit /child1/foo:1)
     *   |
     *   + rev2 (second commit with /child2)
     *   |\
     *   | + branchRev2 (brach from rev2)
     * </pre>
     * The test reads /child from <code>branchRev2</code> and expects
     * the version from the first commit.
     */
    @Test
    public void isolatedBranches() throws Exception {
        String rev1 = mk.commit("", "+\"/child1\":{}", null, "");

        String branchRev1 = mk.branch(rev1);
        mk.commit("/child1", "^\"foo\":1", branchRev1, "");

        String rev2 = mk.commit("", "+\"/child2\":{}", null, "");

        String branchRev2 = mk.branch(rev2);
        String json = mk.getNodes("/child1", branchRev2, 1000, 0, -1, null);
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(json);
        assertFalse(obj.containsKey("foo"));
    }

    @Test
    public void movesInBranch() throws Exception {
        String branchRev = mk.branch(null);
        branchRev = mk.commit("/", "+\"a\":{}", branchRev, null);
        branchRev = mk.commit("/a", "^\"foo\":1", branchRev, null);
        branchRev = mk.commit("/", ">\"a\" : \"b\"", branchRev, null);
        branchRev = mk.commit("/", ">\"b\" : \"a\"", branchRev, null);
        mk.merge(branchRev, null);

        String json = mk.getNodes("/a", null, 0, 0, -1, null);
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(json);
        assertTrue(obj.containsKey("foo"));
    }
}
