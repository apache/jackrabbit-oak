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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.DynamicBoostCommonTest;
import org.junit.ClassRule;
import org.junit.Test;

public class MongotDynamicBoostCommonTest extends DynamicBoostCommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    public MongotDynamicBoostCommonTest() {
        indexOptions = new MongotIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new MongotCommonTestRepositoryBuilder(mongo).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    @Override
    protected String getTestQueryDynamicBoostBasicExplained() {
        return "dynamicBoostTokens";
    }

    @Override
    @Test
    public void dynamicBoostedTagsShouldShouldBeUsedInSimilarityQueries() throws Exception {
        super.dynamicBoostedTagsShouldShouldBeUsedInSimilarityQueries();
    }

    @Test
    public void dynamicBoostAnalyzed() throws Exception {
        createAssetsIndexAndProperties(false, false);

        Tree testParent = createNodeWithType(
                root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");
        Tree predicted1 = createAssetNodeWithPredicted(
                testParent, "asset1", "flower with a lot of red and a bit of blue");
        createPredictedTag(predicted1, "red", 9.0);
        createPredictedTag(predicted1, "blue", 1.0);
        Tree predicted2 = createAssetNodeWithPredicted(
                testParent, "asset2", "flower with a lot of blue and a bit of red");
        createPredictedTag(predicted2, "red", 1.0);
        createPredictedTag(predicted2, "blue", 9.0);
        root.commit();

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'flower')]",
                    XPATH, List.of("/test/asset1", "/test/asset2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] "
                            + "where contains(title, 'red-flower')",
                    List.of("/test/asset1", "/test/asset2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] "
                            + "where contains(title, 'blue-flower')",
                    List.of("/test/asset2", "/test/asset1"));
        });
    }

    @Test
    public void dynamicBoostNotIncludedInFullText() throws Exception {
        createAssetsIndexAndProperties(false, false, false);

        Tree testParent = createNodeWithType(
                root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");
        Tree predicted1 = createAssetNodeWithPredicted(
                testParent, "asset1", "flower with a lot of red and a bit of blue");
        createPredictedTag(predicted1, "fooTag", 100.0);
        createPredictedTag(predicted1, "barTag", 1.0);
        createPredictedTag(predicted1, "red", 9.0);
        createPredictedTag(predicted1, "blue", 1.0);
        Tree predicted2 = createAssetNodeWithPredicted(
                testParent, "asset2", "flower with a lot of blue and a bit of red");
        createPredictedTag(predicted2, "fooTag", 1.0);
        createPredictedTag(predicted2, "barTag", 100.0);
        createPredictedTag(predicted2, "red", 1.0);
        createPredictedTag(predicted2, "blue", 9.0);
        Tree predicted3 = createAssetNodeWithPredicted(
                testParent, "asset3", "this is a not matching asset");
        createPredictedTag(predicted3, "fooTag", 1.0);
        createPredictedTag(predicted3, "barTag", 1.0);
        root.commit();

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'fooTag')]", XPATH, List.of());
            assertOrderedQuery("select [jcr:path] from [dam:Asset] "
                            + "where contains(*, 'flower OR fooTag')",
                    List.of("/test/asset1", "/test/asset2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] "
                            + "where contains(*, 'flower OR barTag')",
                    List.of("/test/asset2", "/test/asset1"));
        });
    }
}
