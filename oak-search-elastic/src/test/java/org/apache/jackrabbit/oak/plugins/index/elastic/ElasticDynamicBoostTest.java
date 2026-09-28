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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.DynamicBoostCommonTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Runs every test twice, once with {@link ElasticDocument#FT_OAK_12353_ENABLE} enabled and once
 * with it disabled, so both dynamic-boost grouping code paths get the same coverage.
 */
@RunWith(Parameterized.class)
public class ElasticDynamicBoostTest extends DynamicBoostCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    @Parameterized.Parameters(name = "dynamicBoostGroupingEnabled={0}")
    public static Iterable<Object[]> fixtures() {
        return List.of(new Object[]{true}, new Object[]{false});
    }

    @Parameterized.Parameter
    public boolean dynamicBoostGroupingEnabled;

    public ElasticDynamicBoostTest() {
        this.indexOptions = new ElasticIndexOptions();
    }

    @Before
    public void setDynamicBoostGroupingToggle() {
        ElasticDocument.FT_OAK_12353_ENABLE.set(dynamicBoostGroupingEnabled);
    }

    @After
    public void resetDynamicBoostGroupingToggle() {
        ElasticDocument.FT_OAK_12353_ENABLE.set(true);
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    @Override
    protected String getTestQueryDynamicBoostBasicExplained() {
        return "{\"_source\":{\"includes\":[\":path\"]}," +
                "\"query\":{\"bool\":{\"must\":[{\"bool\":{\"must\":[{\"query_string\":{\"default_operator\":\"and\"," +
                "\"fields\":[\"title^1.0\",\":dynamic-boost-ft^1.0E-4\",\":fulltext\"],\"lenient\":true,\"query\":\"plant\",\"tie_breaker\":0.5,\"type\":\"cross_fields\"}}]," +
                "\"should\":[{\"nested\":{\"path\":\"predictedTagsDynamicBoost\",\"query\":{\"function_score\":{\"boost\":9.999999747378752E-5," +
                "\"functions\":[{\"field_value_factor\":{\"field\":\"predictedTagsDynamicBoost.boost\"}}]," +
                "\"query\":{\"match\":{\"predictedTagsDynamicBoost.value\":{\"query\":\"plant\"}}}}},\"score_mode\":\"avg\"}}]}}]}}," +
                "\"size\":10,\"sort\":[{\"_score\":{\"order\":\"desc\"}},{\":path\":{\"order\":\"asc\"}}],\"track_total_hits\":10000}";
    }

    /**
     * This test cannot work in Lucene since terms are not analyzed in standard mode, and weight are not evaluated in lite mode
     */
    @Test
    public void dynamicBoostAnalyzed() throws Exception {
        createAssetsIndexAndProperties(false, false);

        Tree testParent = createNodeWithType(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");

        Tree predicted1 = createAssetNodeWithPredicted(testParent, "asset1", "flower with a lot of red and a bit of blue");
        createPredictedTag(predicted1, "red", 9.0);
        createPredictedTag(predicted1, "blue", 1.0);

        Tree predicted2 = createAssetNodeWithPredicted(testParent, "asset2", "flower with a lot of blue and a bit of red");
        createPredictedTag(predicted2, "red", 1.0);
        createPredictedTag(predicted2, "blue", 9.0);

        root.commit();

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'flower')]",
                    XPATH, List.of("/test/asset1", "/test/asset2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(title, 'red-flower')",
                    List.of("/test/asset1", "/test/asset2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(title, 'blue-flower')",
                    List.of("/test/asset2", "/test/asset1"));
        });
    }

    /**
     * Predicted tags sharing the same boost score are grouped into a single nested document
     * (see {@link ElasticDocument#FT_OAK_12353_ENABLE}). This verifies that querying still
     * matches on any of the grouped values, both with the grouping enabled and disabled
     * (see {@link #dynamicBoostGroupingEnabled}).
     */
    @Test
    public void dynamicBoostQueriesGroupedValuesSharingSameBoostScore() throws Exception {
        createAssetsIndexAndProperties(false, false);

        Tree testParent = createNodeWithType(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");

        Tree predicted1 = createAssetNodeWithPredicted(testParent, "asset1", "flower with a lot of red and a bit of blue");
        createPredictedTag(predicted1, "red", 5.0);
        createPredictedTag(predicted1, "blue", 5.0);
        createPredictedTag(predicted1, "green", 5.0);
        createPredictedTag(predicted1, "special", 9.0);

        root.commit();

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'red')]", XPATH, List.of("/test/asset1"));
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'blue')]", XPATH, List.of("/test/asset1"));
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'green')]", XPATH, List.of("/test/asset1"));
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'special')]", XPATH, List.of("/test/asset1"));
        });
    }

    @Test
    public void dynamicBoostNotIncludedInFullText() throws Exception {
        createAssetsIndexAndProperties(false, false, false);

        Tree testParent = createNodeWithType(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");

        Tree predicted1 = createAssetNodeWithPredicted(testParent, "asset1", "flower with a lot of red and a bit of blue");
        createPredictedTag(predicted1, "fooTag", 100.0);
        createPredictedTag(predicted1, "barTag", 1.0);
        createPredictedTag(predicted1, "red", 9.0);
        createPredictedTag(predicted1, "blue", 1.0);

        Tree predicted2 = createAssetNodeWithPredicted(testParent, "asset2", "flower with a lot of blue and a bit of red");
        createPredictedTag(predicted2, "fooTag", 1.0);
        createPredictedTag(predicted2, "barTag", 100.0);
        createPredictedTag(predicted2, "red", 1.0);
        createPredictedTag(predicted2, "blue", 9.0);

        Tree predicted3 = createAssetNodeWithPredicted(testParent, "asset3", "this is a not matching asset");
        createPredictedTag(predicted3, "fooTag", 1.0);
        createPredictedTag(predicted3, "barTag", 1.0);

        root.commit();

        assertEventually(() -> {
            // with this test we are checking that the dynamic boost is not included in the fulltext search
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'fooTag')]", XPATH, List.of());
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'flower OR fooTag')",
                    List.of("/test/asset1", "/test/asset2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'flower OR barTag')",
                    List.of("/test/asset2", "/test/asset1"));
        });

    }

    @Test
    public void ranking() throws Exception {
        createAssetsIndexAndProperties(false, false);
        Tree test = createNodeWithType(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");

        // asset1: three tags sharing one boost group (boost 1)
        Tree many = createAssetNodeWithPredicted(test, "asset1", "titleone");
        createPredictedTag(many, "red", 1.0);
        createPredictedTag(many, "blue", 1.0);
        createPredictedTag(many, "green", 1.0);

        // asset2: one high-boost tag in its own group, the other two effectively zero
        Tree single = createAssetNodeWithPredicted(test, "asset2", "titletwo");
        createPredictedTag(single, "red", 4.0);
        createPredictedTag(single, "blue", 0.01);
        createPredictedTag(single, "green", 0.01);

        root.commit();
        String query =
                "select [jcr:path] from [dam:Asset] where contains(*, 'red blue green')";
        assertEventually(() -> assertOrderedQuery(query, List.of("/test/asset2", "/test/asset1")));
    }
}
