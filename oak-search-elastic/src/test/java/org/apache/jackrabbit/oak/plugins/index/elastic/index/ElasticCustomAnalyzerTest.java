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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import co.elastic.clients.elasticsearch._types.analysis.Analyzer;
import co.elastic.clients.elasticsearch._types.analysis.CustomAnalyzer;
import co.elastic.clients.elasticsearch.indices.IndexSettingsAnalysis;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticCustomAnalyzerTest {

    private static final String CALLER_ANALYZER_NAME = "oak_analyzer";
    private static final String TITLE_ANALYZER_NAME = "titleAnalyzer";

    /**
     * Builds an "analyzers" NodeState with two children:
     * - {@code default}: a simple built-in class analyzer (french)
     * - {@code titleAnalyzer}: a composed analyzer with its own tokenizer and a "LowerCase" filter
     * and verifies that {@link ElasticCustomAnalyzer#buildCustomAnalyzers} registers BOTH analyzers,
     * with the composed analyzer's internal tokenizer/filter names uniquely prefixed.
     */
    @Test
    public void buildCustomAnalyzersRegistersAllNamedAnalyzers() {
        NodeBuilder analyzersBuilder = EmptyNodeState.EMPTY_NODE.builder();
        Tree analyzers = TreeFactory.createTree(analyzersBuilder);

        Tree defaultAnalyzer = analyzers.addChild(FulltextIndexConstants.ANL_DEFAULT);
        defaultAnalyzer.setProperty(FulltextIndexConstants.ANL_NAME, "french");

        Tree titleAnalyzer = analyzers.addChild(TITLE_ANALYZER_NAME);
        titleAnalyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
        Tree filters = titleAnalyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
        filters.setOrderableChildren(true);
        filters.addChild("LowerCase");

        NodeState analyzersState = analyzersBuilder.getNodeState();

        IndexSettingsAnalysis.Builder builder = ElasticCustomAnalyzer.buildCustomAnalyzers(analyzersState, CALLER_ANALYZER_NAME);
        assertNotNull("builder should not be null when analyzers node has children", builder);

        IndexSettingsAnalysis settings = builder.build();

        // both the default (aliased to the caller-supplied name) and the extra named analyzer must be registered
        assertEquals(Set.of(CALLER_ANALYZER_NAME, TITLE_ANALYZER_NAME), settings.analyzer().keySet());

        Analyzer titleAnalyzerDef = settings.analyzer().get(TITLE_ANALYZER_NAME);
        assertTrue("titleAnalyzer should be a composed (custom) analyzer", titleAnalyzerDef.isCustom());
        CustomAnalyzer customAnalyzer = titleAnalyzerDef.custom();

        // the internal tokenizer name must be prefixed with the JCR analyzer name, not the old shared literal
        String tokenizerName = customAnalyzer.tokenizer();
        assertEquals(TITLE_ANALYZER_NAME + "_tokenizer", tokenizerName);
        assertTrue("prefixed tokenizer definition must round-trip into the settings' tokenizer map",
                settings.tokenizer().containsKey(tokenizerName));

        // the internal filter name(s) must be prefixed with the JCR analyzer name too
        assertEquals(1, customAnalyzer.filter().size());
        String filterName = customAnalyzer.filter().get(0);
        assertTrue("filter name must be prefixed with the analyzer's own name",
                filterName.startsWith(TITLE_ANALYZER_NAME + "_"));
        assertTrue("prefixed filter definition must round-trip into the settings' filter map",
                settings.filter().containsKey(filterName));

        // no char filters were configured for titleAnalyzer
        assertTrue(customAnalyzer.charFilter().isEmpty());
    }
}
