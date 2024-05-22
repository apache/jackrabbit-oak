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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

public abstract class DynamicBoostCommonTest extends AbstractQueryTest {

    protected static final String ASSET_NODE_TYPE = "[dam:Asset]\n" + " - * (UNDEFINED) multiple\n" + " - * (UNDEFINED)\n" + " + * (nt:base) = oak:TestNode VERSION";

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Test
    public void basicQuery() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertThat(explain("//element(*, dam:Asset)[jcr:contains(., 'plant')]", XPATH),
                containsString(getTestQueryDynamicBoostBasicExplained()));

        assertEventually(() -> {
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'plant')]", XPATH,
                    List.of("/test/asset1", "/test/asset2", "/test/asset3"));
            assertQuery("//element(*, dam:Asset)[jcr:contains(., 'flower')]", XPATH, List.of("/test/asset1", "/test/asset2"));
        });
    }

    @Test
    public void dynamicBoostWithMultipleTerms() throws Exception {
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
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'red flower')",
                    List.of("/test/asset1", "/test/asset2"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue flower')",
                    List.of("/test/asset2", "/test/asset1"));
        });
    }

    @Test
    public void caseInsensitive() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertEventually(() ->
                assertQuery("//element(*, dam:Asset)[jcr:contains(., 'FLOWER')]", XPATH, List.of("/test/asset1", "/test/asset2")));

    }

    @Test
    public void boostOrder() throws Exception {
        createAssetsIndexAndProperties(false, false);
        prepareTestAssets();

        assertEventually(() ->
                assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'plant')",
                        List.of("/test/asset2", "/test/asset3", "/test/asset1")));
    }

    @Test
    public void wildcardQueries() throws Exception {
        boolean lite = areAnalyzeFeaturesSupportedInLiteModeOnly();
        createAssetsIndexAndProperties(lite, lite);
        prepareTestAssets();

        assertEventually(() -> {
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blu?')", SQL2, List.of("/test/asset3"));
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'bl?e')", SQL2, List.of("/test/asset3"));
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, '?lue')", SQL2, List.of("/test/asset3"));
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'coff*')", SQL2, List.of("/test/asset2"));
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'co*ee')", SQL2, List.of("/test/asset2"));
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, '*ffee')", SQL2, List.of("/test/asset2"));
        });
    }

    @Test
    public void queryWithExplicitOr() throws Exception {
        boolean lite = areAnalyzeFeaturesSupportedInLiteModeOnly();
        createAssetsIndexAndProperties(lite, lite);
        prepareTestAssets();

        assertEventually(() -> {
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue OR flower')", SQL2,
                    List.of("/test/asset1", "/test/asset2", "/test/asset3"));
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'blue OR coffee')", SQL2,
                    List.of("/test/asset2", "/test/asset3"));
        });
    }

    @Test
    public void queryWithMinus() throws Exception {
        boolean lite = areAnalyzeFeaturesSupportedInLiteModeOnly();
        createAssetsIndexAndProperties(lite, lite);
        prepareTestAssets();

        assertEventually(() -> {
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'plant -flower')", SQL2, List.of("/test/asset3"));
            assertQuery("select [jcr:path] from [dam:Asset] where contains(*, 'flower -coffee')", SQL2, List.of("/test/asset1"));
        });

    }

    @Test
    public void dynamicBoostShouldGiveLessRelevanceToTags() throws Exception {
        boolean lite = areAnalyzeFeaturesSupportedInLiteModeOnly();
        createAssetsIndexAndProperties(lite, lite);
        prepareTestAssets();

        assertEventually(() -> {
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'titleone OR blue')",
                    List.of("/test/asset1", "/test/asset3"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'short OR coffee')",
                    List.of("/test/asset3", "/test/asset2"));
        });
    }

    @Test
    public void dynamicBoostShouldNotMatchOnSingleFields() throws Exception {
        boolean lite = areAnalyzeFeaturesSupportedInLiteModeOnly();
        createAssetsIndexAndProperties(lite, lite);
        prepareTestAssets();

        assertEventually(() -> {
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(*, 'long')",
                    List.of("/test/asset1", "/test/asset2", "/test/asset3"));
            assertOrderedQuery("select [jcr:path] from [dam:Asset] where contains(title, 'long')",
                    List.of("/test/asset1", "/test/asset2"));
        });
    }

    @Test
    public void dynamicBoostedTagsShouldShouldBeUsedInSimilarityQueries() throws Exception {
        boolean lite = areAnalyzeFeaturesSupportedInLiteModeOnly();
        createAssetsIndexAndProperties(lite, lite);
        prepareTestAssets();

        assertEventually(() -> assertOrderedQuery("select [jcr:path] from [dam:Asset] where similar(., '/test/asset1')",
                List.of("/test/asset1", "/test/asset2", "/test/asset3")));
    }

    protected abstract String getTestQueryDynamicBoostBasicExplained();

    protected boolean areAnalyzeFeaturesSupportedInLiteModeOnly() {
        return false;
    }

    protected void prepareTestAssets() throws CommitFailedException {
        Tree testParent = createNodeWithType(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED, "");

        Tree predicted1 = createAssetNodeWithPredicted(testParent, "asset1", "titleone long");
        predicted1.setProperty("jcr:mixinTypes", List.of("mix:referenceable"), Type.STRINGS);
        predicted1.setProperty("jcr:uuid", UUID.randomUUID().toString());
        createPredictedTag(predicted1, "plant", 0.1);
        createPredictedTag(predicted1, "flower", 0.1);

        Tree predicted2 = createAssetNodeWithPredicted(testParent, "asset2", "titletwo long");
        predicted2.setProperty("jcr:mixinTypes", List.of("mix:referenceable"), Type.STRINGS);
        predicted2.setProperty("jcr:uuid", UUID.randomUUID().toString());
        createPredictedTag(predicted2, "plant", 0.9);
        createPredictedTag(predicted2, "flower", 0.1);
        createPredictedTag(predicted2, "coffee", 0.5);

        Tree predicted3 = createAssetNodeWithPredicted(testParent, "asset3", "titlethree short");
        predicted3.setProperty("jcr:mixinTypes", List.of("mix:referenceable"), Type.STRINGS);
        predicted3.setProperty("jcr:uuid", UUID.randomUUID().toString());
        createPredictedTag(predicted3, "plant", 0.5);
        createPredictedTag(predicted3, "blue", 0.5);
        createPredictedTag(predicted3, "long", 0.1);
        root.commit();
    }

    protected Tree createAssetNodeWithPredicted(Tree parent, String assetNodeName, String assetTitle) {
        Tree node = createNodeWithType(parent, assetNodeName, "dam:Asset", assetTitle);
        return createNodeWithType(
                createNodeWithType(createNodeWithType(node, JcrConstants.JCR_CONTENT, JcrConstants.NT_UNSTRUCTURED, ""), "metadata",
                        JcrConstants.NT_UNSTRUCTURED, ""), "predictedTags", JcrConstants.NT_UNSTRUCTURED, "");
    }

    protected void createPredictedTag(Tree parent, String tagName, double confidence) {
        Tree node = createNodeWithType(parent, tagName, JcrConstants.NT_UNSTRUCTURED, "");
        node.setProperty("name", tagName);
        node.setProperty("confidence", confidence);
    }

    protected void createAssetsIndexAndProperties(boolean lite, boolean similarityTags) throws Exception {
        NodeTypeRegistry.register(root, new ByteArrayInputStream(ASSET_NODE_TYPE.getBytes()), "test nodeType");
        Tree indexRuleProps = createIndex("dam:Asset", lite);

        Tree predictedTagsDynamicBoost = createNodeWithType(indexRuleProps, "predictedTagsDynamicBoost", JcrConstants.NT_UNSTRUCTURED, "");
        predictedTagsDynamicBoost.setProperty("name", "jcr:content/metadata/predictedTags/.*");
        predictedTagsDynamicBoost.setProperty("isRegexp", true);
        predictedTagsDynamicBoost.setProperty("dynamicBoost", true);

        if (similarityTags) {
            Tree predictedTags = createNodeWithType(indexRuleProps, "predictedTags", JcrConstants.NT_UNSTRUCTURED, "");
            predictedTags.setProperty("name", "jcr:content/metadata/predictedTags/*/name");
            predictedTags.setProperty("isRegexp", true);
            predictedTags.setProperty("similarityTags", true);
        }

        root.commit();
    }

    protected Tree createIndex(String nodeType, boolean lite) {
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), nodeType, false);
        builder.noAsync();
        builder.evaluatePathRestrictions();
        builder.indexRule(nodeType)
                .property("title")
                .propertyIndex()
                .nodeScopeIndex().analyzed();

        Tree index = indexOptions.setIndex(root, TEST_INDEX_NAME, builder);
        index.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        index.setProperty(IndexConstants.DYNAMIC_BOOST_LITE_PROPERTY_NAME, lite);
        return TestUtil.newRulePropTree(index, nodeType);
    }

    protected Tree createNodeWithType(Tree parent, String nodeName, String nodeType, String title) {
        Tree node = parent.addChild(nodeName);
        node.setProperty(JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME);
        if (StringUtils.isNotEmpty(title)) {
            node.setProperty("title", title, Type.STRING);
        }
        return node;
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    protected String explain(String query, String lang) {
        String explain = "explain " + query;
        return executeQuery(explain, lang).get(0);
    }

    protected void assertOrderedQuery(String sql, List<String> paths) {
        List<String> result = executeQuery(sql, AbstractQueryTest.SQL2, true, true);
        assertEquals(paths, result);
    }

}
