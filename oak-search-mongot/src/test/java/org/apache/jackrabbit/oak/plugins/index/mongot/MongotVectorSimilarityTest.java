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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.bson.Document;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MongotVectorSimilarityTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @After
    public void resetMongoDatabase() {
        mongo.useFreshDatabase();
    }

    @Test
    public void ranksRepSimilarResultsFromReferenceVector() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        Tree vector = builder.definition().indexRule("nt:unstructured").property("fv")
                .type("Binary").useInSimilarity(true).nodeScopeIndex()
                .similaritySearchDenseVectorSize(3).getBuilderTree();
        vector.setProperty("similarityMetric", "cosine");

        NodeBuilder test = builder.root().child("test");
        addVector(test, "a", 1.0f, 0.0f, 0.0f);
        addVector(test, "b", 0.9f, 0.1f, 0.0f);
        addVector(test, "c", 0.0f, 1.0f, 0.0f);

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            Document reference = fixture.collection().find(
                    new Document(MongoFieldNames.PATH, "/test/a")).first();
            String vectorField = FieldNames.createSimilarityFieldName(
                    MongoFieldNames.encodeProperty("fv"));
            assertNotNull(reference);
            assertEquals(List.of(1.0d, 0.0d, 0.0d),
                    reference.getList(vectorField, Double.class));

            String query = "select [jcr:path] from [nt:unstructured] "
                    + "where similar(., '/test/a')";

            fixture.assertMongotPlan(query, "JCR-SQL2");
            String plan = fixture.query("explain " + query, "JCR-SQL2")
                    .getRows().iterator().next().getValue("plan").getValue(Type.STRING);
            assertTrue(plan, plan.contains("vectorSearch"));
            fixture.awaitPaths(query, "JCR-SQL2",
                    List.of("/test/a", "/test/b", "/test/c"));
        }
    }

    @Test
    public void filtersVectorSimilarityWithFullTextConstraint() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        Tree vector = builder.definition().indexRule("nt:unstructured").property("fv")
                .type("Binary").useInSimilarity(true).nodeScopeIndex()
                .similaritySearchDenseVectorSize(3).getBuilderTree();
        vector.setProperty("similarityMetric", "cosine");
        builder.definition().indexRule("nt:unstructured").property("text")
                .analyzed().nodeScopeIndex();

        NodeBuilder test = builder.root().child("test");
        addVector(test, "a", 1.0f, 0.0f, 0.0f).setProperty("text", "hello");
        addVector(test, "b", 0.9f, 0.1f, 0.0f).setProperty("text", "hello");
        addVector(test, "c", 0.8f, 0.2f, 0.0f).setProperty("text", "world");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            String query = "select [jcr:path] from [nt:unstructured] "
                    + "where contains(*, 'hello') and similar(., '/test/a')";

            fixture.assertMongotPlan(query, "JCR-SQL2");
            fixture.awaitPaths(query, "JCR-SQL2", List.of("/test/a", "/test/b"));
        }
    }

    private static NodeBuilder addVector(NodeBuilder parent, String name, float... values) {
        ByteBuffer bytes = ByteBuffer.allocate(values.length * Float.BYTES);
        for (float value : values) {
            bytes.putFloat(value);
        }
        NodeBuilder child = parent.child(name);
        child
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("fv", new ArrayBasedBlob(bytes.array()), Type.BINARY);
        return child;
    }
}
