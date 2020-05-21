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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticsearchIndexHelperTest {

    @Test
    public void multiRulesWithSamePropertyNames() throws IOException {
        IndexDefinitionBuilder builder = new ElasticsearchIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRuleA = builder.indexRule("typeA");
        indexRuleA.property("foo").type("String");
        IndexDefinitionBuilder.IndexRule indexRuleB = builder.indexRule("typeB");
        indexRuleB.property("foo").type("String").analyzed();
        NodeState nodeState = builder.build();

        ElasticsearchIndexDefinition definition =
                new ElasticsearchIndexDefinition(nodeState, nodeState, "path", "prefix");

        CreateIndexRequest request = ElasticsearchIndexHelper.createIndexRequest(definition);

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonMap = mapper.readValue(request.mappings().streamInput(), Map.class);

        Map fooMapping = (Map) ((Map) jsonMap.get("properties")).get("foo");
        assertThat(fooMapping.get("type"), is("text"));
        Map fooKeywordMapping = (Map) ((Map) fooMapping.get("fields")).get("keyword");
        assertThat(fooKeywordMapping.get("type"), is("keyword"));
    }

    @Test(expected = IllegalStateException.class)
    public void multiRulesWithSamePropertyNamesDifferentTypes() throws IOException {
        IndexDefinitionBuilder builder = new ElasticsearchIndexDefinitionBuilder();
        IndexDefinitionBuilder.IndexRule indexRuleA = builder.indexRule("typeA");
        indexRuleA.property("foo").type("String");
        IndexDefinitionBuilder.IndexRule indexRuleB = builder.indexRule("typeB");
        indexRuleB.property("foo").type("Boolean");
        NodeState nodeState = builder.build();

        ElasticsearchIndexDefinition definition =
                new ElasticsearchIndexDefinition(nodeState, nodeState, "path", "prefix");

        ElasticsearchIndexHelper.createIndexRequest(definition);
    }

}
