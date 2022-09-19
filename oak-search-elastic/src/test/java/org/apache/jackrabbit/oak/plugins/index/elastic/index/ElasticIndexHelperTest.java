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

import co.elastic.clients.elasticsearch._types.analysis.TokenFilter;
import co.elastic.clients.elasticsearch._types.analysis.TokenFilterDefinition;
import co.elastic.clients.elasticsearch._types.analysis.WordDelimiterGraphTokenFilter;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexSettingsAnalysis;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticIndexHelperTest {

    @Test
    public void multiRulesWithSamePropertyNames() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRuleA = builder.indexRule("typeA");
        indexRuleA.property("foo").type("String");
        IndexDefinitionBuilder.IndexRule indexRuleB = builder.indexRule("typeB");
        indexRuleB.property("foo").type("String").analyzed();
        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest("prefix.path", definition);

        TypeMapping fooPropertyMappings = request.mappings();
        assertThat(fooPropertyMappings, notNullValue());
        Property fooProperty = fooPropertyMappings.properties().get("foo");
        assertThat(fooProperty, is(notNullValue()));
        assertThat(fooProperty._kind(), is(Property.Kind.Text));
        TextProperty fooTextProperty = fooProperty.text();

        Property keywordField = fooTextProperty.fields().get("keyword");
        assertThat(keywordField, is(notNullValue()));
        assertThat(keywordField._kind(), is(Property.Kind.Keyword));
    }

    @Test(expected = IllegalStateException.class)
    public void multiRulesWithSamePropertyNamesDifferentTypes() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRuleA = builder.indexRule("typeA");
        indexRuleA.property("foo").type("String");
        IndexDefinitionBuilder.IndexRule indexRuleB = builder.indexRule("typeB");
        indexRuleB.property("foo").type("Boolean");
        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        ElasticIndexHelper.createIndexRequest("prefix.path", definition);
    }

    @Test
    public void oakAnalyzer() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("type");
        indexRule.property("foo").type("String").analyzed();
        indexRule.property("bar").type("String");

        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest("prefix.path", definition);

        checkAnalyzerPreservesOriginalTerm(request, false);

        TypeMapping fooMappings = request.mappings();
        assertThat(fooMappings, notNullValue());
        Property fooProperty = fooMappings.properties().get("foo");
        assertThat(fooProperty, is(notNullValue()));
        TextProperty textProperty = fooProperty.text();
        assertThat(textProperty.analyzer(), is("oak_analyzer"));
        Property keywordField = textProperty.fields().get("keyword");
        assertThat(keywordField._kind(), is(Property.Kind.Keyword));

        TypeMapping barMappings = request.mappings();
        assertThat(barMappings, notNullValue());
        Property barProperty = barMappings.properties().get("bar");
        assertThat(barProperty._kind(), is(Property.Kind.Keyword));
    }

    @Test
    public void oakAnalyzerWithOriginalTerm() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("type");
        indexRule.property("foo").type("String").analyzed();
        Tree analyzer = builder.getBuilderTree().addChild("analyzers");
        analyzer.setProperty("indexOriginalTerm", "true");

        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest("prefix.path", definition);
        checkAnalyzerPreservesOriginalTerm(request, true);
    }

    private void checkAnalyzerPreservesOriginalTerm(CreateIndexRequest request, boolean expected) {
        IndexSettings requestSettings = request.settings();
        assertThat(requestSettings, notNullValue());
        IndexSettingsAnalysis analysisSettings = requestSettings.analysis();
        assertThat(analysisSettings, notNullValue());
        TokenFilter filter = analysisSettings.filter().get("oak_word_delimiter_graph_filter");
        assertThat(filter, notNullValue());
        TokenFilterDefinition tokenFilterDefinition = filter.definition();
        assertThat(tokenFilterDefinition._kind(), is(TokenFilterDefinition.Kind.WordDelimiterGraph));
        WordDelimiterGraphTokenFilter wdg = tokenFilterDefinition.wordDelimiterGraph();
        assertThat(wdg.preserveOriginal(), is(expected));
    }
}
