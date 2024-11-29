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
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

public class ElasticDynamicBoostTest extends DynamicBoostCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticDynamicBoostTest() {
        this.indexOptions = new ElasticIndexOptions();
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
                "\"fields\":[\"title^1.0\",\":dynamic-boost-ft^1.0E-4\",\":fulltext\"],\"query\":\"plant\",\"tie_breaker\":0.5,\"type\":\"cross_fields\"}}]," +
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
}
