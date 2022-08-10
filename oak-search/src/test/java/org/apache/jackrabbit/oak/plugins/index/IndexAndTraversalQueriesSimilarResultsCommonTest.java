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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class IndexAndTraversalQueriesSimilarResultsCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r, ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        // This is necessary so that queries without indexes will return results
        setTraversalEnabled(true);
    }

    protected Tree setIndex(IndexDefinitionBuilder builder, String idxName) {
        return builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    protected List<String> passingQueries;

    protected List<String> failingQueries;

    @Test
    public void runPassingQueries() throws Exception {
        runQueries(passingQueries, false);
    }

    @Ignore("OAK-9874")
    @Test
    public void runFailingQueries() throws Exception {
        runQueries(failingQueries, true);
    }

    private void runQueries(List<String> testQueries, boolean shouldAllFail) throws Exception {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("long").setProperty("propa", 1234);
        test.addChild("double").setProperty("propa", 1.11);
        test.addChild("string_numeric").setProperty("propa", "1234");
        test.addChild("string").setProperty("propa", "1234a");
        test.addChild("boolean_true").setProperty("propa", true);
        test.addChild("boolean_false").setProperty("propa", false);
        test.addChild("other").setProperty("propb", "another property");
        root.commit();

        HashMap<String, List<String>> resultsWithoutIndex = new HashMap<>();
        HashMap<String, List<String>> resultsWithIndex = new HashMap<>();

        // Run all queries without an index and collect the results
        for (String query : testQueries) {
            List<String> results = executeQuery(query, XPATH, true);
            resultsWithoutIndex.put(query, results);
        }

        // Create an index on propa of type Long.
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base").property("propa").type("Long").propertyIndex();
        setIndex(builder, "propa-index");
        root.commit();

        assertEventually(() -> {
            List<String> result = executeQuery("/jcr:root//*[@propa]", XPATH, true);
            assertFalse(result.isEmpty());
        });

        // Rerun the queries and collect results
        for (String query : testQueries) {
            List<String> results = executeQuery(query, XPATH, true);
            resultsWithIndex.put(query, results);
        }

        ArrayList<String> queriesFailed = new ArrayList<>();
        ArrayList<String> queriesPassed = new ArrayList<>();
        // Compare the results for all queries
        StringBuilder sb = new StringBuilder();
        for (String query : testQueries) {
            List<String> resultWithIndex = resultsWithIndex.get(query);
            List<String> resultWithoutIndex = resultsWithoutIndex.get(query);
            Collections.sort(resultWithIndex);
            Collections.sort(resultWithoutIndex);

            if (resultWithIndex.equals(resultWithoutIndex)) {
                queriesPassed.add(query);
            } else {
                queriesFailed.add(query);
                sb.append(String.format("Query results differ.\n  Query:         %s\n  With index:    %s\n  Without index: %s\n",
                        query, resultWithIndex, resultWithoutIndex));
            }
        }
        if (shouldAllFail) {
            assertTrue("Expecting all queries to fail, but these have passed: " + queriesPassed,
                    queriesPassed.isEmpty());
        }
        assertTrue("Some queries results differ when run with and without index:\n" + sb,
                queriesFailed.isEmpty());
    }
}
