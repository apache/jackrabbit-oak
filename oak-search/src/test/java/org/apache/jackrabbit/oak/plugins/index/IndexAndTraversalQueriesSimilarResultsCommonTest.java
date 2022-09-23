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

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class IndexAndTraversalQueriesSimilarResultsCommonTest extends AbstractQueryTest {
    private static final Logger LOG = LoggerFactory.getLogger(IndexAndTraversalQueriesSimilarResultsCommonTest.class);

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        // This is necessary so that queries without indexes will return results
        setTraversalEnabled(true);
    }

    protected Tree setIndex(IndexDefinitionBuilder builder, String idxName) {
        return builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(idxName));
    }

    /*
     * There are three ways to execute a query:
     * - Traversal (no index)
     * - Lucene
     * - Elastic
     * They should all produce the same results, but this is currently not the case. This test compares the results with
     * traversal and the results with using an index (Lucene or Elastic, see the subclasses).
     *
     * Comments:
     * - This test does not check if the results are "correct" (it does not include expected results). It
     * only checks that traversal produces the same results as with an index.
     *
     * - There are two test methods, one for queries which already produce the same results (and, by transitivity, the
     * same result with Elastic and Lucene), and another for queries that have different results. Both tests are enabled
     * but the one for queries with different results tests that they do produce different results. This is to help keeping
     * the test up-to-date, because this test will fail if for some reason, a query starts producing the same results.
     * At this point, we can move the query to the ones that produce the same results.
     *
     * - Currently, the two sets of queries (same and different results) are the same for Elastic and Lucene, so these
     * lists are defined in the base class of the test. But it is possible that in the future these list differ between
     * Elastic and Lucene. In this case, we can move the definition of the lists to the base class.
     *
     * - The queries that have different results between traversal and the indexes, for the most part also produce
     * different results between Elastic and Lucene.
     */
    protected List<String> queriesWithSameResults = ImmutableList.of(
            // Full-text queries
            "/jcr:root//*[jcr:contains(@propa, '*')]",
            "/jcr:root//*[jcr:contains(@propa, '123*')]",
            "/jcr:root//*[jcr:contains(@propa, 'fal*')]"
    );

    protected List<String> queriesWithDifferentResults = ImmutableList.of(
            "/jcr:root//*[@propa]",
            "/jcr:root//*[@propa > 0]",
            "/jcr:root//*[@propa > '0']",
            "/jcr:root//*[@propa = 1.11]",
            "/jcr:root//*[@propa = '1.11']",
            "/jcr:root//*[@propa > 1]",
            "/jcr:root//*[@propa > '1']",
            "/jcr:root//*[@propa > 1111]",
            "/jcr:root//*[@propa > '1111']",
            "/jcr:root//*[@propa = true]",
            "/jcr:root//*[@propa = 'true']",
            "/jcr:root//*[@propa = false]",
            "/jcr:root//*[@propa = 'false']"
    );

    @Test
    public void runQueriesWithSameResults() throws Exception {
        runQueries(queriesWithSameResults, false);
    }

    @Test
    public void runQueriesWithDifferentResults() throws Exception {
        runQueries(queriesWithDifferentResults, true);
    }

    /**
     * @param shouldAllFail If all queries should fail.
     */
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

        ArrayList<String> queriesWithDifferentResults = new ArrayList<>();
        ArrayList<String> queriesWithSameResults = new ArrayList<>();
        // Compare the results for all queries
        StringBuilder sb = new StringBuilder();
        for (String query : testQueries) {
            List<String> resultWithIndex = resultsWithIndex.get(query);
            List<String> resultWithoutIndex = resultsWithoutIndex.get(query);
            Collections.sort(resultWithIndex);
            Collections.sort(resultWithoutIndex);

            if (resultWithIndex.equals(resultWithoutIndex)) {
                queriesWithSameResults.add(query);
            } else {
                queriesWithDifferentResults.add(query);
                sb.append(String.format("Query results differ.\n  Query:         %s\n  With index:    %s\n  Without index: %s\n",
                        query, resultWithIndex, resultWithoutIndex));
            }
        }
        if (shouldAllFail) {
            LOG.info("The following queries produced different results with traversal and with index:\n{}", sb);
            assertTrue("Expecting all queries to fail, but these have passed: " + queriesWithSameResults,
                    queriesWithSameResults.isEmpty());
        } else {
            assertTrue("Some queries results differ when run with and without index:\n" + sb,
                    queriesWithDifferentResults.isEmpty());
        }
    }
}
