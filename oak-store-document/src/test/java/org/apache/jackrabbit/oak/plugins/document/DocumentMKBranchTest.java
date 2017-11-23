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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <code>DocumentMKBranchTest</code> performs a test to check if commits
 * to a branch are not visible to other branches.
 */
public class DocumentMKBranchTest extends BaseDocumentMKTest {

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
     *
     * @throws ParseException
     */
    @Test
    public void isolatedBranches() throws ParseException {
        String rev1 = mk.commit("", "+\"/child1\":{}", null, "");

        String branchRev1 = mk.branch(rev1);
        mk.commit("/child1", "^\"foo\":1", branchRev1, "");

        String rev2 = mk.commit("", "+\"/child2\":{}", null, "");

        String branchRev2 = mk.branch(rev2);
        String json = mk.getNodes("/child1", branchRev2, 0, 0, -1, null);
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

    @Test
    public void branchIsolation1() throws Exception {
        String filter = "{\"properties\":[\"*\",\":hash\",\":id\"]}";
        String baseRev = mk.commit("/", "+\"test\":{\"node\":{}}", null, null);

        // branch commit under /test/node
        String branchRev = mk.branch(baseRev);
        String branchRev1 = mk.commit("/test/node", "+\"branch-node\":{}", branchRev, null);

        // trunk commit under /test/node
        mk.commit("/test/node", "+\"trunk-node\":{}", null, null);

        // branch commit on /
        String branchRev2 = mk.commit("/", "+\"other\":{}", branchRev1, null);

        // get /test on branch and use returned identifier to get next level
        String json = mk.getNodes("/test", branchRev2, 0, 0, 1000, filter);

        JSONObject test = parseJSONObject(json);
        String id = resolveValue(test, ":id").toString();
        String revision = id.split("@")[1];
        assertNodesExist(revision, "/test/node/branch-node");
        assertNodesNotExist(revision, "/test/node/trunk-node");
    }

    @Test
    public void branchIsolation2() throws Exception {
        String filter = "{\"properties\":[\"*\",\":hash\",\":id\"]}";
        String baseRev = mk.commit("/", "+\"test\":{\"node\":{}}", null, null);
        String branchRev = mk.branch(baseRev);

        // trunk commit under /test/node
        String rev1 = mk.commit("/test/node", "+\"trunk-node\":{}", null, null);

        // branch commit under /test/node
        mk.commit("/test/node", "+\"branch-node\":{}", branchRev, null);

        // trunk commit on /
        String rev2 = mk.commit("/", "+\"other\":{}", null, null);

        // get /test on trunk and use returned identifier to get next level
        String json = mk.getNodes("/test", rev2, 0, 0, 1000, filter);

        JSONObject test = parseJSONObject(json);
        String id = resolveValue(test, ":id").toString();
        String revision = id.split("@")[1];
        assertEquals(rev1, revision);
        assertNodesExist(revision, "/test/node/trunk-node");
        assertNodesNotExist(revision, "/test/node/branch-node");
    }
}
