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
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class ElasticIndexHelperTest {

    @Test
    public void manyFields() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        builder.getBuilderTree().setProperty(ElasticIndexDefinition.LIMIT_TOTAL_FIELDS, 1234L);
        IndexDefinitionBuilder.IndexRule indexRuleA = builder.indexRule("typeA");
        indexRuleA.property("foo").type("String");
        NodeState nodeState = builder.build();
        ElasticIndexDefinition definition =
            new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");
        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest("prefix.path", definition);
        assertEquals(1234L, Long.parseLong(request.settings().index().mapping().totalFields().limit()));
        assertEquals(true, request.settings().index().mapping().ignoreMalformed());
    }

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
        Property fooProperty = fooPropertyMappings.properties().get(ElasticIndexUtils.fieldName("foo"));
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
    public void analyzerWithEmptyTokenizer() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("idxRule");
        indexRule.property("foo").type("String").useInSimilarity();

        Tree analyzer = builder.getBuilderTree().addChild("analyzers");
        Tree defaultAnalyzer = analyzer.addChild("default");
        defaultAnalyzer.setProperty(FulltextIndexConstants.ANL_CLASS, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        defaultAnalyzer.addChild("tokenizer");
        defaultAnalyzer.addChild("filters");

        NodeState nodeState = builder.build();
        ElasticIndexDefinition definition =
            new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");
        ElasticIndexHelper.createIndexRequest("prefix.path", definition);
    }

    @Test
    public void analyzerWithEmptyDefault() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("idxRule");
        indexRule.property("foo").type("String").useInSimilarity();

        Tree analyzer = builder.getBuilderTree().addChild("analyzers");
        analyzer.addChild("default");

        NodeState nodeState = builder.build();
        ElasticIndexDefinition definition =
            new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");
        ElasticIndexHelper.createIndexRequest("prefix.path", definition);
    }

    @Test
    public void analyzerWithWordDelimiter() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("idxRule");
        indexRule.property("foo").type("String").useInSimilarity();

        Tree analyzer = builder.getBuilderTree().addChild("analyzers");
        Tree defaultAnalyzer = analyzer.addChild("default");
        Tree tokenizer = defaultAnalyzer.addChild("tokenizer");
        tokenizer.setProperty("name", "Standard");
        Tree filters = defaultAnalyzer.addChild("filters");
        filters.addChild("LowerCase");
        filters.addChild("WordDelimiter");
        Tree synonym = filters.addChild("Synonym");
        synonym.setProperty("format", "solr");
        synonym.setProperty("ignoreCase", true);
        synonym.setProperty("synonyms", "synonyms.txt");
        Tree synonymsText = filters.addChild("synonyms.txt");
        Tree synonymsContent = synonymsText.addChild("jcr:content");
        synonymsContent.setProperty("jcr:data", "test");
        filters.addChild("PorterStem");

        NodeState nodeState = builder.build();
        ElasticIndexDefinition definition =
            new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");
        ElasticIndexHelper.createIndexRequest("prefix.path", definition);
    }

    @Test()
    public void indexSettingsAreCorrectlySet() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("idxRule");
        indexRule.property("foo").type("String").useInSimilarity();

        final String expectedNumberOfShards = "2";
        final boolean expectedIndexOriginalTerm = true;
        final boolean expectedSplitOnCaseChange = true;
        final boolean expectedSplitOnNumerics = true;

        Tree analyzer = builder.getBuilderTree().addChild("analyzers");
        analyzer.setProperty(FulltextIndexConstants.INDEX_ORIGINAL_TERM, expectedIndexOriginalTerm);
        analyzer.setProperty(ElasticIndexDefinition.SPLIT_ON_CASE_CHANGE, expectedSplitOnCaseChange);
        analyzer.setProperty(ElasticIndexDefinition.SPLIT_ON_NUMERICS, expectedSplitOnNumerics);

        NodeState nodeState = builder.build();

        @NotNull NodeState defn = nodeState.builder()
            .setProperty(ElasticIndexDefinition.NUMBER_OF_SHARDS, expectedNumberOfShards)
            .getNodeState();

        ElasticIndexDefinition definition =
            new ElasticIndexDefinition(nodeState, defn, "path", "prefix");
        CreateIndexRequest req = ElasticIndexHelper.createIndexRequest("prefix.path", definition);

        IndexSettings indexSettings = req.settings().index();
        assertThat(expectedNumberOfShards, is(indexSettings.numberOfShards()));

        WordDelimiterGraphTokenFilter wdgfDef = req.settings()
            .analysis()
            .filter().get("oak_word_delimiter_graph_filter")
            .definition()
            .wordDelimiterGraph();
        assertThat(wdgfDef.preserveOriginal(), is(expectedIndexOriginalTerm));
        assertThat(wdgfDef.splitOnCaseChange(), is(expectedSplitOnCaseChange));
        assertThat(wdgfDef.splitOnNumerics(), is(expectedSplitOnNumerics));
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
        Property fooProperty = fooMappings.properties().get(ElasticIndexUtils.fieldName("foo"));
        assertThat(fooProperty, is(notNullValue()));
        TextProperty textProperty = fooProperty.text();
        assertThat(textProperty.analyzer(), is("oak_analyzer"));
        Property keywordField = textProperty.fields().get("keyword");
        assertThat(keywordField._kind(), is(Property.Kind.Keyword));

        TypeMapping barMappings = request.mappings();
        assertThat(barMappings, notNullValue());
        Property barProperty = barMappings.properties().get(ElasticIndexUtils.fieldName("bar"));
        assertThat(barProperty._kind(), is(Property.Kind.Keyword));
    }

    @Test
    public void oakAnalyzerWithOriginalTerm() {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("type");
        indexRule.property("foo").type("String").analyzed();
        Tree analyzer = builder.getBuilderTree().addChild("analyzers");
        analyzer.setProperty(FulltextIndexConstants.INDEX_ORIGINAL_TERM, "true");

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

    @Test
    public void testJsonToMapping() {
        // Test basic field types
        String json = "{\n" +
            "  \"properties\": {\n" +
            "    \"text_field\": { \"type\": \"text\", \"analyzer\": \"standard\" },\n" +
            "    \"keyword_field\": { \"type\": \"keyword\", \"index\": true },\n" +
            "    \"date_field\": { \"type\": \"date\" },\n" +
            "    \"boolean_field\": { \"type\": \"boolean\" },\n" +
            "    \"numeric_field\": { \"type\": \"long\" }\n" +
            "  }\n" +
            "}";

        Map<String, Property> propertyMap = ElasticIndexHelper.jsonToMapping(json);

        // Verify the mapping was parsed correctly
        assertThat(propertyMap, notNullValue());
        assertThat(propertyMap.size(), is(5));

        // Text field verification
        Property textField = propertyMap.get("text_field");
        assertThat(textField, notNullValue());
        assertThat(textField._kind(), is(Property.Kind.Text));
        assertThat(textField.text().analyzer(), is("standard"));

        // Keyword field verification
        Property keywordField = propertyMap.get("keyword_field");
        assertThat(keywordField, notNullValue());
        assertThat(keywordField._kind(), is(Property.Kind.Keyword));

        // Date field verification
        Property dateField = propertyMap.get("date_field");
        assertThat(dateField, notNullValue());
        assertThat(dateField._kind(), is(Property.Kind.Date));

        // Boolean field verification
        Property booleanField = propertyMap.get("boolean_field");
        assertThat(booleanField, notNullValue());
        assertThat(booleanField._kind(), is(Property.Kind.Boolean));

        // Numeric field verification
        Property numericField = propertyMap.get("numeric_field");
        assertThat(numericField, notNullValue());
        assertThat(numericField._kind(), is(Property.Kind.Long));
    }

    @Test
    public void testJsonToMappingWithNestedFields() {
        // Test nested object fields
        String json = "{\n" +
            "  \"properties\": {\n" +
            "    \"nested_obj\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"properties\": {\n" +
            "        \"inner_text\": { \"type\": \"text\" },\n" +
            "        \"inner_keyword\": { \"type\": \"keyword\" }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        Map<String, Property> propertyMap = ElasticIndexHelper.jsonToMapping(json);

        // Verify the mapping was parsed correctly
        assertThat(propertyMap, notNullValue());
        assertThat(propertyMap.size(), is(1));

        // Object field verification
        Property nestedObj = propertyMap.get("nested_obj");
        assertThat(nestedObj, notNullValue());
        assertThat(nestedObj._kind(), is(Property.Kind.Object));

        // Verify nested properties
        Map<String, Property> nestedProperties = nestedObj.object().properties();
        assertThat(nestedProperties.size(), is(2));

        Property innerText = nestedProperties.get("inner_text");
        assertThat(innerText, notNullValue());
        assertThat(innerText._kind(), is(Property.Kind.Text));

        Property innerKeyword = nestedProperties.get("inner_keyword");
        assertThat(innerKeyword, notNullValue());
        assertThat(innerKeyword._kind(), is(Property.Kind.Keyword));
    }

    @Test
    public void testParseJsonToPropertyMap() {
        String json = "{\n" +
            "\"properties\": {\n" +
            "          \"processingTimeMs\": {\n" +
            "            \"type\": \"date\"\n" +
            "          },\n" +
            "          \"latestError\": {\n" +
            "            \"type\": \"keyword\",\n" +
            "            \"index\": false\n" +
            "          },\n" +
            "          \"errorCount\": {\n" +
            "            \"type\": \"short\"\n" +
            "          },\n" +
            "          \"status\": {\n" +
            "            \"type\": \"keyword\"\n" +
            "          }\n" +
            "        }\n" +
            "    }";


        Map<String, Property> propertyMap = ElasticIndexHelper.jsonToMapping(json);

        // Verify the mapping was parsed correctly
        assertThat(propertyMap, notNullValue());
        assertThat(propertyMap.size(), is(4));

        // Processing time verification
        Property processingTime = propertyMap.get("processingTimeMs");
        assertThat(processingTime, notNullValue());
        assertThat(processingTime._kind(), is(Property.Kind.Date));

        // Latest error verification
        Property latestError = propertyMap.get("latestError");
        assertThat(latestError, notNullValue());
        assertThat(latestError._kind(), is(Property.Kind.Keyword));
        assertThat(latestError.keyword().index(), is(false));

        // Error count verification
        Property errorCount = propertyMap.get("errorCount");
        assertThat(errorCount, notNullValue());
        assertThat(errorCount._kind(), is(Property.Kind.Short));

        // Status verification
        Property status = propertyMap.get("status");
        assertThat(status, notNullValue());
        assertThat(status._kind(), is(Property.Kind.Keyword));
    }

}
