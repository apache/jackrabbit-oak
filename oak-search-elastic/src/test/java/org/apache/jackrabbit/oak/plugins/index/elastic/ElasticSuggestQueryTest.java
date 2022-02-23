/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import java.io.ByteArrayInputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElasticSuggestQueryTest extends ElasticAbstractQueryTest {
    private static final String ASSET_NODE_TYPE = "[dam:Asset]\n" + " - * (UNDEFINED) multiple\n" + " - * (UNDEFINED)\n" + " + * (nt:base) = oak:TestNode VERSION";
    private static final String NODE_TYPE_ASSET = "dam:Asset";
    private static final String SUGGEST_TITLE = "this is just a sample text which should get some response in suggestions";

    @Test
    public void testQuerySuggest() throws Exception {
        configureSuggestIndex();
        prepareTestAssets();
        String suggestQuery = "SELECT [rep:suggest()] FROM [dam:Asset] as s WHERE SUGGEST('this')";
        assertEventually(() -> {
            assertResultList(executeSuggestQuery(suggestQuery, SQL2),
                    Arrays.asList(SUGGEST_TITLE + "-1", SUGGEST_TITLE + "-2", SUGGEST_TITLE + "-3"));
            Assert.assertTrue(explain(suggestQuery, SQL2).contains(
                    "{\"bool\":{\"must\":[{\"bool\":{\"must\":[{\"nested\":{\"query\":{\"match_bool_prefix\":"
                            + "{\":suggest.value\":{\"query\":\"this\",\"operator\":\"AND\",\"prefix_length\":0,\"max_expansions\":50,"
                            + "\"fuzzy_transpositions\":true,\"boost\":1.0}}},\"path\":\":suggest\",\"ignore_unmapped\":false,"
                            + "\"score_mode\":\"max\",\"boost\":1.0,\"inner_hits\":{\"ignore_unmapped\":false,\"from\":0,\"size\":100,"
                            + "\"version\":false,\"seq_no_primary_term\":false,\"explain\":false,\"track_scores\":false}}},"
                            + "{\"bool\":{\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],"
                            + "\"adjust_pure_negative\":true,\"boost\":1.0}}"));
        });
    }

    protected static void assertResultList(@NotNull List<String> expected, @NotNull List<String> actual) {
        for (String p : checkNotNull(expected)) {
            assertTrue("Expected path " + p + " not found, got " + actual, checkNotNull(actual).contains(p));
        }
        assertEquals("Result set size is different: " + actual, expected.size(), actual.size());
    }

    private List<String> executeSuggestQuery(String query, String language) {
        List<String> results = new ArrayList<String>();
        Result result = null;
        try {
            result = executeQuery(query, language, NO_BINDINGS);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        for (ResultRow row : result.getRows()) {
            results.add(row.getValue("rep:suggest()").toString());
        }
        return results;
    }

    // utils
    private void configureSuggestIndex() throws CommitFailedException {
        NodeTypeRegistry.register(root, new ByteArrayInputStream(ASSET_NODE_TYPE.getBytes()), "Asset nodeType");
        IndexDefinitionBuilder builder = createIndex(true, NODE_TYPE_ASSET, "title", "suggestTest");
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule(NODE_TYPE_ASSET);
        IndexDefinitionBuilder.PropertyRule titlePropertyRule = indexRule.property("title").analyzed().nodeScopeIndex().useInSuggest();
        titlePropertyRule.getBuilderTree().setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);

        setIndex("damAsset_" + UUID.randomUUID(), builder);
        root.commit();
    }

    private void prepareTestAssets() throws CommitFailedException {
        Tree testParent = createNodeWithType(root.getTree("/"), "test", NT_UNSTRUCTURED);
        createAssetNode(testParent, "asset1", SUGGEST_TITLE + "-1");
        createAssetNode(testParent, "asset2", SUGGEST_TITLE + "-2");
        createAssetNode(testParent, "asset3", SUGGEST_TITLE + "-3");
        root.commit();
    }

    private Tree createAssetNode(Tree parent, String nodeName, String title) {
        Tree item = createNodeWithType(parent, nodeName, NODE_TYPE_ASSET);
        item.setProperty("title", title);
        return item;
    }

    private Tree createNodeWithType(Tree parent, String nodeName, String typeName) {
        parent = parent.addChild(nodeName);
        parent.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return parent;
    }
}
