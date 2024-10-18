/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonData;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ElasticInferenceTest extends ElasticAbstractQueryTest {

    @Test
    public void inferenceConfigStoredInIndexMetadata() throws CommitFailedException {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        Tree inferenceConfig = builder.getBuilderTree().addChild(ElasticIndexDefinition.INFERENCE_CONFIG);
        Tree embeddings = inferenceConfig.addChild("embeddings");
        embeddings.setProperty("serviceUrl", "http://localhost:8080");
        embeddings.setProperty("fields", List.of("a"), Type.STRINGS);
        builder.indexRule("nt:base").property("a").analyzed();
        String indexName = UUID.randomUUID().toString();
        Tree index = setIndex(indexName, builder);
        root.commit();

        ElasticIndexDefinition definition = getElasticIndexDefinition(index);

        IndexMappingRecord mapping = getMapping(index);
        Map<String, JsonData> meta = mapping.mappings().meta();
        JsonData inferenceConfigJson = meta.get(ElasticIndexDefinition.INFERENCE_CONFIG);
        assertNotNull(inferenceConfigJson);
        ElasticIndexDefinition.InferenceDefinition inferenceDefinition = inferenceConfigJson.to(ElasticIndexDefinition.InferenceDefinition.class);
        assertEquals(inferenceDefinition, definition.inferenceDefinition);
    }
}
