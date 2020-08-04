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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNameHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticIndexHelperTest {

    @Test
    public void multiRulesWithSamePropertyNames() throws IOException {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRuleA = builder.indexRule("typeA");
        indexRuleA.property("foo").type("String");
        IndexDefinitionBuilder.IndexRule indexRuleB = builder.indexRule("typeB");
        indexRuleB.property("foo").type("String").analyzed();
        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest(ElasticIndexNameHelper.getRemoteIndexName(definition), definition);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue(request.mappings().streamInput(), Map.class);

        Map fooMapping = (Map) ((Map) jsonMap.get("properties")).get("foo");
        assertThat(fooMapping.get("type"), is("text"));
        Map fooKeywordMapping = (Map) ((Map) fooMapping.get("fields")).get("keyword");
        assertThat(fooKeywordMapping.get("type"), is("keyword"));
    }

    @Test(expected = IllegalStateException.class)
    public void multiRulesWithSamePropertyNamesDifferentTypes() throws IOException {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRuleA = builder.indexRule("typeA");
        indexRuleA.property("foo").type("String");
        IndexDefinitionBuilder.IndexRule indexRuleB = builder.indexRule("typeB");
        indexRuleB.property("foo").type("Boolean");
        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        ElasticIndexHelper.createIndexRequest(ElasticIndexNameHelper.getRemoteIndexName(definition), definition);
    }

    @Test
    public void oakAnalyzer() throws IOException {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("type");
        indexRule.property("foo").type("String").analyzed();
        indexRule.property("bar").type("String");

        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest(ElasticIndexNameHelper.getRemoteIndexName(definition), definition);

        assertThat(request.settings().get("analysis.filter.oak_word_delimiter_graph_filter.preserve_original"), is("false"));

        assertThat(request.settings().get("analysis.filter.shingle.type"), nullValue());
        assertThat(request.settings().get("analysis.filter.analyzer.trigram.type"), nullValue());

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMappings = mapper.readValue(request.mappings().streamInput(), Map.class);
        Map fooMapping = (Map) ((Map) jsonMappings.get("properties")).get("foo");
        assertThat(fooMapping.get("analyzer"), is("oak_analyzer"));
        Map barMapping = (Map) ((Map) jsonMappings.get("properties")).get("bar");
        assertThat(barMapping.get("analyzer"), nullValue());
    }

    @Test
    public void oakAnalyzerWithOriginalTerm() throws IOException {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("type");
        indexRule.property("foo").type("String").analyzed();
        Tree analyzer = builder.getBuilderTree().addChild("analyzers");
        analyzer.setProperty("indexOriginalTerm", "true");

        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest(ElasticIndexNameHelper.getRemoteIndexName(definition), definition);

        assertThat(request.settings().get("analysis.filter.oak_word_delimiter_graph_filter.preserve_original"), is("true"));
    }

    @Test
    public void spellCheck() throws IOException {
        IndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("type");
        indexRule.property("foo").type("String").analyzed().useInSpellcheck();
        indexRule.property("bar").type("String").analyzed();

        NodeState nodeState = builder.build();

        ElasticIndexDefinition definition =
                new ElasticIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticIndexHelper.createIndexRequest(ElasticIndexNameHelper.getRemoteIndexName(definition), definition);

        assertThat(request.settings().get("analysis.filter.shingle.type"), is("shingle"));
        assertThat(request.settings().get("analysis.analyzer.trigram.type"), is("custom"));

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMappings = mapper.readValue(request.mappings().streamInput(), Map.class);
        Map fooMapping = (Map) ((Map) jsonMappings.get("properties")).get("foo");
        Map fooFields = (Map) fooMapping.get("fields");
        assertThat(fooFields.get("trigram"), notNullValue());
        Map barMapping = (Map) ((Map) jsonMappings.get("properties")).get("bar");
        Map barFields = (Map) barMapping.get("fields");
        assertThat(barFields.get("trigram"), nullValue());
    }

}
