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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toByteArray;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toFloats;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import co.elastic.clients.elasticsearch._types.mapping.DenseVectorProperty;
import co.elastic.clients.elasticsearch._types.mapping.FieldMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.indices.GetFieldMappingResponse;
import co.elastic.clients.elasticsearch.indices.get_field_mapping.TypeFieldMappings;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ElasticSimilarQueryTest extends ElasticAbstractQueryTest {

    @Test
    public void repSimilarWithStopWords() throws Exception {
        createIndex(true);

        String nativeQueryStringWithStopWords = "select [jcr:path] from [nt:base] where " +
                "native('elastic-sim', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0&mlt.stopwords=Hello,bye')";

        String nativeQueryStringWithoutStopWords = "select [jcr:path] from [nt:base] where " +
                "native('elastic-sim', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0&mlt.minshouldmatch=20%')";

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World. Ok Bye Bye now. See you tomorrow.");
        test.addChild("b").setProperty("text", "He said Hello and then the she said Hello as well.");
        test.addChild("c").setProperty("text", "He said Bye.");
        test.addChild("d").setProperty("text", "Bye Bye World.");
        test.addChild("e").setProperty("text", "See you Tomorrow");
        test.addChild("f").setProperty("text", "Hello Mr X. Let's catch up tomorrow. Bye Bye");
        test.addChild("g").setProperty("text", "Random text");
        root.commit();

        // Matches due to terms Hello or bye should be ignored
        assertEventually(() -> assertQuery(nativeQueryStringWithStopWords, List.of("/test/a", "/test/e", "/test/f")));

        assertEventually(() -> assertQuery(nativeQueryStringWithoutStopWords,
                List.of("/test/a", "/test/b", "/test/c", "/test/d", "/test/e", "/test/f")));
    }

    @Test
    public void repSimilarWithMinWordLength() throws Exception {
        createIndex(true);
        String nativeQueryStringWithMinWordLength = "select [jcr:path] from [nt:base] where " +
                "native('elastic-sim', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0&mlt.minwl=6')";

        String nativeQueryStringWithoutMinWordLength = "select [jcr:path] from [nt:base] where " +
                "native('elastic-sim', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0')";

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello Worlds.");
        test.addChild("b").setProperty("text", "He said Hello and then the world said Hello as well.");
        test.addChild("c").setProperty("text", "War of the worlds is a good movie");
        test.addChild("d").setProperty("text", "Hello. How are you? Worlds");
        root.commit();

        // Matches because of term Hello should be ignored since wl <6 (so /test/ should NOT be in the match list)
        // /test/d should be in match list (because of Worlds term)
        assertEventually(() -> assertQuery(nativeQueryStringWithMinWordLength, List.of("/test/a", "/test/c", "/test/d")));

        assertEventually(() -> assertQuery(nativeQueryStringWithoutMinWordLength,
                List.of("/test/a", "/test/b", "/test/c", "/test/d")));
    }

    @Test
    public void repSimilarQueryWithLongPath() throws Exception {
        createIndex(false);
        Tree test = root.getTree("/").addChild("test");
        Tree longPath = test.addChild("a");
        for (int i = 0; i < 258; i++) {
            longPath = longPath.addChild("a" + i);
        }
        longPath.setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "Bye Bye");
        test.addChild("f").setProperty("text", "Hello");
        test.addChild("g").setProperty("text", "World");
        test.addChild("h").setProperty("text", "Hello");
        root.commit();

        final String p = longPath.getPath();
        String query = "select [jcr:path] from [nt:base] where similar(., '" + p + "')";

        assertEventually(() -> assertQuery(query,
                List.of(p, "/test/b", "/test/c", "/test/d", "/test/f", "/test/g", "/test/h")));
    }

    /**
     * This test checks <a href="https://github.com/elastic/elasticsearch/pull/94518">94518</a> issue.
     */
    @Test
    public void repSimilarQueryWithIgnoredMetadataField() throws Exception {
        createIndex(false);
        Tree test = root.getTree("/").addChild("test");

        // the max keyword length is 256, this field will be then listed as _ignored
        test.addChild("a").setProperty("text", ElasticTestUtils.randomString(1000));
        root.commit();

        String query = "select [jcr:path] from [nt:base] where similar(., '/test/a')";

        assertEventually(() -> assertQuery(query, List.of("/test/a")));
    }

    @Test
    public void similarityTagsAffectRelevance() throws Exception {
        createIndex(false);

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("text", "Hello World Hello World");
        a.setProperty("tags", "foo");
        Tree b = test.addChild("b");
        b.setProperty("text", "Hello World Hello World");
        b.setProperty("tags", "bar");
        Tree c = test.addChild("c");
        c.setProperty("text", "Hello World Hello World");
        c.setProperty("tags", "foo");
        root.commit();

        assertEventually(() -> {
            List<String> paths = executeQuery("select [jcr:path] from [nt:base] where similar(., '/test/a')", SQL2, true, true);
            assertEquals(paths.size(), 3);
            assertEquals(paths.get(2), "/test/b");
        });
    }

    @Test
    public void vectorSimilarityIndexConfiguration() throws Exception {
        final String indexName = "test1";
        final String fieldName1 = "fv1";
        final String similarityFieldName1 = FieldNames.createSimilarityFieldName(fieldName1);
        IndexDefinitionBuilder builder = createIndex(fieldName1);
        Tree tree = builder.indexRule("nt:base").property(fieldName1).useInSimilarity(true).nodeScopeIndex()
                .similaritySearchDenseVectorSize(2048).getBuilderTree();
        tree.setProperty(ElasticPropertyDefinition.PROP_SIMILARITY_METRIC, "cosine");

        Tree index = setIndex(indexName, builder);
        root.commit();

        GetFieldMappingResponse mappingsResponse = esConnection.getClient()
                .indices()
                .getFieldMapping(b -> b
                        .index(getElasticIndexDefinition(index).getIndexAlias())
                        .fields(similarityFieldName1)
                );

        Map<String, TypeFieldMappings> mappings = mappingsResponse.result();
        assertEquals("More than one index found", 1, mappings.size());
        Map<String, FieldMapping> typeFieldMappings = mappings.entrySet().iterator().next().getValue().mappings();
        Property v = typeFieldMappings.get(similarityFieldName1).mapping().get(similarityFieldName1);
        DenseVectorProperty denseVector = v.denseVector();
        assertEquals("Dense vector size doesn't match", 2048, denseVector.dims().intValue());
        assertEquals("Similarity metric doesn't match", "l2_norm", denseVector.similarity());
    }

    @Test
    public void vectorSimilarityWithWrongVectorSizes() throws Exception {
        IndexDefinitionBuilder builder = createIndex("fv");
        builder.indexRule("nt:base").property("fv").useInSimilarity(true).nodeScopeIndex()
                .similaritySearchDenseVectorSize(100);// test FVs have size 1048
        Tree index = setIndex("test1", builder);
        root.commit();
        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        for (String line : IOUtils.readLines(Files.newInputStream(file.toPath()), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Float> values = Stream.of(split).skip(1).map(Float::parseFloat).collect(Collectors.toList());
            byte[] bytes = toByteArray(values);
            List<Float> actual = toFloats(bytes);
            assertEquals(values, actual);

            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            String name = split[0];
            Tree child = test.addChild(name);
            child.setProperty("fv", blob, Type.BINARY);
        }
        root.commit();

        // regardless of the wrong vectors, we should be able to index
        assertEventually(() -> assertEquals(10, countDocuments(index)));
    }

    @Test
    public void vectorSimilarity() throws Exception {
        IndexDefinitionBuilder builder = createIndex("fv");
        builder.indexRule("nt:base").property("fv").useInSimilarity(true).nodeScopeIndex();
        setIndex("test1", builder);
        root.commit();
        Tree test = root.getTree("/").addChild("test");

        URI uri = getClass().getResource("/org/apache/jackrabbit/oak/query/fvs.csv").toURI();
        File file = new File(uri);

        List<String> children = new LinkedList<>();
        for (String line : IOUtils.readLines(Files.newInputStream(file.toPath()), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Float> values = Stream.of(split).skip(1).map(Float::parseFloat).collect(Collectors.toList());
            byte[] bytes = toByteArray(values);
            List<Float> actual = toFloats(bytes);
            assertEquals(values, actual);

            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            String name = split[0];
            Tree child = test.addChild(name);
            child.setProperty("fv", blob, Type.BINARY);
            children.add(child.getPath());
        }

        root.commit();

        // check that similarity changes across different feature vectors
        List<String> baseline = new LinkedList<>();
        for (String similarPath : children) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + similarPath + "')";
            List<String> current = new LinkedList<>();
            assertEventually(() -> {
                Iterator<String> result = executeQuery(query, "JCR-SQL2", false, true).iterator();
                current.clear();
                while (result.hasNext()) {
                    String next = result.next();
                    current.add(next);
                }
                assertNotEquals(baseline, current);
            });
            baseline.clear();
            baseline.addAll(current);
        }
    }

    private void verifyLSHResults(Map<String, List<String>> expectedResults, double expected, double delta) {
        for (String similarPath : expectedResults.keySet()) {
            String query = "select [jcr:path] from [nt:base] where similar(., '" + "/test/" + similarPath + "')";
            assertEventually(() -> {
                Iterator<String> result = executeQuery(query, "JCR-SQL2", false, true).iterator();
                List<String> expectedList = expectedResults.get(similarPath.substring(similarPath.lastIndexOf("/") + 1));
                List<String> found = new ArrayList<>();
                int resultNum = 0;
                // Verify that the expected results are present in the top 10 results
                while (result.hasNext() && resultNum < expectedList.size()) {
                    String next = result.next();
                    next = next.substring(next.lastIndexOf("/") + 1);
                    found.add(next);
                    resultNum++;
                }
                double per = (expectedList.stream().filter(found::contains).count() * 100.0) / expectedList.size();
                assertEquals("expected: " + expectedList + " got: " + found, expected, per, delta);
            });
        }
    }

    @Test
    public void vectorSimilarityLargeData() throws Exception {

        final int similarImageCount = 10;
        int featureVectorLength = 1024;

        IndexDefinitionBuilder builder = createIndex("fv");
        builder.indexRule("nt:base").property("fv").useInSimilarity(true).nodeScopeIndex();

        setIndex("test1", builder);
        root.commit();
        Tree test = root.getTree("/").addChild("test");

        Random r = new Random(1);
        ArrayList<String> imageNameList = new ArrayList<>();
        ArrayList<float[]> imageDataList = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            String imageName = "img" + i;
            imageNameList.add(imageName);
            List<Float> values = new ArrayList<>();
            float[] imageData = new float[featureVectorLength];
            for (int j = 0; j < featureVectorLength; j++) {
                float x = r.nextFloat() * 0.5f;
                float g = (float) (30 * Math.pow(x, 3));
                values.add(g);
                imageData[j] = g;
            }
            imageDataList.add(imageData);
            byte[] bytes = toByteArray(values);
            List<Float> actual = toFloats(bytes);
            assertEquals(values, actual);
            Blob blob = root.createBlob(new ByteArrayInputStream(bytes));
            Tree child = test.addChild(imageName);
            child.setProperty("fv", blob, Type.BINARY);
        }
        root.commit();

        Map<String, List<String>> expectedResults = new HashMap<>();
        for (int testCase = 0; testCase < 10; testCase++) {
            int imageId = r.nextInt(imageDataList.size());
            float[] find = imageDataList.get(imageId);
            String imageName = imageNameList.get(imageId);
            ArrayList<Image> images = new ArrayList<>();
            for (int i = 0; i < imageDataList.size(); i++) {
                Image img = new Image();
                img.name = imageNameList.get(i);
                float[] compare = imageDataList.get(i);
                img.distance = euclideanDistance(find, compare);
                images.add(img);
            }
            images.sort(Comparator.comparingDouble(o -> o.distance));
            ArrayList<String> expected = new ArrayList<>();
            for (int i = 0; i < similarImageCount; i++) {
                expected.add(images.get(i).name);
            }
            expectedResults.put(imageName, expected);
        }
        verifyLSHResults(expectedResults, 65, 35);
    }

    static long euclideanDistance(float[] x, float[] y) {
        long sum = 0;
        for (int i = 0; i < x.length; i++) {
            float xx = y[i];
            float yy = x[i];
            float diff = xx - yy;
            sum += diff * diff;
        }
        return sum;
    }

    private void createIndex(boolean nativeQuery) throws Exception {
        IndexDefinitionBuilder builder = createIndex("text", "tags");
        if (nativeQuery) {
            builder.getBuilderTree().setProperty(FulltextIndexConstants.FUNC_NAME, "elastic-sim");
        }
        builder.indexRule("nt:base").property("text").analyzed();
        builder.indexRule("nt:base").property("tags").similarityTags(true);
        String indexId = UUID.randomUUID().toString();
        setIndex(indexId, builder);
        root.commit();
    }

    static class Image {
        double distance;
        String name;
    }

}
