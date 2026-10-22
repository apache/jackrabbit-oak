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

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.pool.TypePool;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.InferenceDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.InferenceDefinition.Property;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.InferenceDefinition.Query;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.osgi.annotation.versioning.Version;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class ElasticInferenceCompatibilityTest {

    @Test
    public void legacyInferenceApiMustBeRemovedInVersion3() {
        // @Version has CLASS retention, so inspect the compiled package annotation.
        AnnotationDescription version = TypePool.Default.of(ElasticIndexDefinition.class.getClassLoader())
                .describe(ElasticIndexDefinition.class.getPackageName() + ".package-info")
                .resolve().getDeclaredAnnotations().ofType(Version.class);
        assertNotNull("The Elasticsearch package must declare its export version", version);
        ElasticSemVer packageVersion = ElasticSemVer.fromString(version.getValue("value").resolve(String.class));
        assertTrue("Remove the deprecated index-local inference API and its compatibility tests "
                        + "at package version 3.0.0 or later (current: " + packageVersion + ")",
                packageVersion.compareTo(new ElasticSemVer(3, 0, 0)) < 0);
    }

    @Test
    public void legacyConfigurationRemainsReadable() {
        ElasticIndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        builder.getBuilderTree().addChild(ElasticIndexDefinition.INFERENCE_CONFIG)
                .addChild("properties").addChild("embeddings");
        NodeState state = builder.build();
        ElasticIndexDefinition definition = new ElasticIndexDefinition(state, state, "/oak:index/test", "prefix");

        assertTrue(definition.isExternallyModifiable());
        assertEquals(ElasticIndexDefinition.INFERENCE + ".embeddings", definition.inferenceDefinition.properties.get(0).name);
        assertNull(definition.inferenceDefinition.queries);

        ElasticIndexDefinition withoutLegacy = new ElasticIndexDefinition(
                EmptyNodeState.EMPTY_NODE, new ElasticIndexDefinitionBuilder().build(), "/oak:index/test", "prefix");
        assertFalse(withoutLegacy.isExternallyModifiable());
        assertNull(withoutLegacy.inferenceDefinition);
    }

    @Test
    public void propertyDefaultsAndOverrides() {
        Property defaults = new Property("embeddings", EmptyNodeState.EMPTY_NODE);
        assertEquals(":inference.embeddings", defaults.name);
        assertEquals("semantic", defaults.model);
        assertEquals(List.of(), defaults.fields);
        assertEquals(1024, defaults.dims);
        assertEquals("cosine", defaults.similarity);

        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty("model", "custom");
        node.setProperty("fields", List.of("title", "description"), Type.STRINGS);
        node.setProperty("dims", 128);
        node.setProperty("similarity", "dot_product");
        Property property = new Property("custom", node.getNodeState());
        assertEquals(":inference.custom", property.name);
        assertEquals("custom", property.model);
        assertEquals(List.of("title", "description"), property.fields);
        assertEquals(128, property.dims);
        assertEquals("dot_product", property.similarity);
    }

    @Test
    public void queryDefaultsAndOverrides() {
        Query defaults = new Query("semantic", EmptyNodeState.EMPTY_NODE);
        assertEquals("semantic", defaults.name);
        assertNull(defaults.serviceUrl);
        assertEquals("semantic", defaults.model);
        assertNull(defaults.prefix);
        assertEquals(2, defaults.minTerms);
        assertEquals(100, defaults.numCandidates);
        assertEquals("hybrid", defaults.type);
        assertEquals(0.5f, defaults.similarityThreshold, 0);
        assertEquals(5000L, defaults.timeout);
        assertTrue(defaults.isEligibleForInput("two terms"));
        assertEquals("two terms", defaults.rewrite("two terms"));
        assertTrue(defaults.hasMinTerms("two terms"));
        assertFalse(defaults.hasMinTerms("one"));

        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty("serviceUrl", "http://localhost/embeddings");
        node.setProperty("model", "custom");
        node.setProperty("prefix", "!");
        node.setProperty("minTerms", 3);
        node.setProperty("numCandidates", 50);
        node.setProperty("type", "vector");
        node.setProperty("similarityThreshold", 0.75);
        node.setProperty("timeout", 1000L);
        Query query = new Query("custom", node.getNodeState());
        assertEquals("custom", query.name);
        assertEquals("http://localhost/embeddings", query.serviceUrl);
        assertEquals("custom", query.model);
        assertEquals("!", query.prefix);
        assertEquals(3, query.minTerms);
        assertEquals(50, query.numCandidates);
        assertEquals("vector", query.type);
        assertEquals(0.75f, query.similarityThreshold, 0);
        assertEquals(1000L, query.timeout);
        assertTrue(query.isEligibleForInput("!three query terms"));
        assertFalse(query.isEligibleForInput("three query terms"));
        assertEquals("three query terms", query.rewrite("!three query terms"));
        assertTrue(query.hasMinTerms("three query terms"));
        assertFalse(query.hasMinTerms("two terms"));
        assertEquals("Query{name='custom', serviceUrl='http://localhost/embeddings', model='custom', "
                + "prefix='!', minTerms=3, numCandidates=50, type='vector', similarityThreshold=0.75, timeout=1000}",
                query.toString());
    }

    @Test
    public void legacyModelRoundTripAndEquality() throws Exception {
        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        node.child("properties").child("embeddings");
        node.child("queries").child("semantic").setProperty("prefix", "!");
        InferenceDefinition original = new InferenceDefinition(node.getNodeState());
        ObjectMapper mapper = new ObjectMapper();
        InferenceDefinition copy = mapper.readValue(mapper.writeValueAsString(original), InferenceDefinition.class);

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertTrue(original.equals(original));
        assertNotEquals(original, null);
        assertNotEquals(original, "other");
        assertNotEquals(original, new InferenceDefinition(EmptyNodeState.EMPTY_NODE));
        assertNull(new InferenceDefinition().properties);
        assertNull(new InferenceDefinition().queries);

        Property property = original.properties.get(0);
        Property propertyCopy = copy.properties.get(0);
        assertEquals(property, propertyCopy);
        assertEquals(property.hashCode(), propertyCopy.hashCode());
        assertTrue(property.equals(property));
        assertNotEquals(property, null);
        assertNotEquals(property, "other");
        propertyCopy.dims++;
        assertNotEquals(property, propertyCopy);

        Query query = original.queries.get(0);
        Query queryCopy = copy.queries.get(0);
        assertEquals(query, queryCopy);
        assertEquals(query.hashCode(), queryCopy.hashCode());
        assertTrue(query.equals(query));
        assertNotEquals(query, null);
        assertNotEquals(query, "other");
        queryCopy.timeout++;
        assertNotEquals(query, queryCopy);
    }
}
