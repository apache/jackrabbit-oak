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

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toByteArray;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils.toDoubles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ElasticSimilarQueryTest extends ElasticAbstractQueryTest {

    /*
    This test mirror the test org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexQueryTest#testRepSimilarAsNativeQuery
    Exact same test data, to test out for feature parity
    The only difference is the same query in lucene returns the doc itself (the one that we need similar docs of) as part of search results
    whereas in elastic, it doesn't.
     */
    @Test
    public void repSimilarAsNativeQuery() throws Exception {

        createIndex(true);

        String nativeQueryString = "select [jcr:path] from [nt:base] where " +
                "native('elastic-sim', 'mlt?stream.body=/test/c&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World");
        test.addChild("b").setProperty("text", "He said Hello and then the world said Hello as well.");
        test.addChild("c").setProperty("text", "He said Hi.");
        root.commit();

        assertEventually(() -> assertQuery(nativeQueryString, Collections.singletonList("/test/b")));
    }

    /*
    This test mirror the test org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexQueryTest#testRepSimilarQuery
    Exact same test data, to test out for feature parity
    The only difference is the same query in lucene returns the doc itself (the one that we need similar docs of) as part of search results
    whereas in elastic, it doesn't.
     */
    @Test
    public void repSimilarQuery() throws Exception {
        createIndex(false);

        String query = "select [jcr:path] from [nt:base] where similar(., '/test/a')";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "Bye Bye");
        test.addChild("f").setProperty("text", "Hello");
        test.addChild("g").setProperty("text", "World");
        test.addChild("h").setProperty("text", "Hello");
        root.commit();

        assertEventually(() -> assertQuery(query,
                Arrays.asList("/test/b", "/test/c", "/test/d", "/test/f", "/test/g", "/test/h")));
    }

    /*
    This test mirror the test org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexQueryTest#testRepSimilarXPathQuery
    Exact same test data, to test out for feature parity
    The only difference is the same query in lucene returns the doc itself (the one that we need similar docs of) as part of search results
    whereas in elastic, it doesn't.
     */
    @Test
    public void repSimilarXPathQuery() throws Exception {
        createIndex(false);

        String query = "//element(*, nt:base)[rep:similar(., '/test/a')]";
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World Hello World");
        test.addChild("b").setProperty("text", "Hello World");
        test.addChild("c").setProperty("text", "World");
        test.addChild("d").setProperty("text", "Hello");
        test.addChild("e").setProperty("text", "Bye Bye");
        test.addChild("f").setProperty("text", "Hello");
        test.addChild("g").setProperty("text", "World");
        test.addChild("h").setProperty("text", "Hello");
        root.commit();
        assertEventually(() -> assertQuery(query, XPATH,
                Arrays.asList("/test/b", "/test/c", "/test/d", "/test/f", "/test/g", "/test/h")));
    }

    @Test
    public void repSimilarWithStopWords() throws Exception {
        createIndex(true);

        String nativeQueryStringWithStopWords = "select [jcr:path] from [nt:base] where " +
                "native('elastic-sim', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0&mlt.stopwords=Hello,bye')";

        String nativeQueryStringWithouStopWords =  "select [jcr:path] from [nt:base] where " +
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
        assertEventually(() -> assertQuery(nativeQueryStringWithStopWords,
                Arrays.asList("/test/e", "/test/f")));

        assertEventually(() -> assertQuery(nativeQueryStringWithouStopWords,
                Arrays.asList("/test/b", "/test/c", "/test/d", "/test/e", "/test/f")));
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
        // /test/d should be in match list (becuase of Worlds term)
        assertEventually(() -> assertQuery(nativeQueryStringWithMinWordLength,
                Arrays.asList("/test/c", "/test/d")));

        assertEventually(() -> assertQuery(nativeQueryStringWithoutMinWordLength,
                Arrays.asList("/test/b", "/test/c", "/test/d")));

    }

    @Test
    public void repSimilarQueryWithLongPath() throws Exception {
        createIndex(false);
        Tree test = root.getTree("/").addChild("test");
        Tree longPath = test.addChild("a");
        for (int i = 0; i < 258; i ++) {
            longPath = longPath.addChild("a"+i);
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

        String query = "select [jcr:path] from [nt:base] where similar(., '"+longPath.getPath()+"')";

        assertEventually(() -> assertQuery(query,
                Arrays.asList("/test/b", "/test/c", "/test/d", "/test/f", "/test/g", "/test/h")));
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

        assertEventually(() -> assertOrderedQuery("select [jcr:path] from [nt:base] where similar(., '/test/a')",
                Arrays.asList("/test/c", "/test/b")));
        assertEventually(() -> assertOrderedQuery("select [jcr:path] from [nt:base] where similar(., '/test/c')",
                Arrays.asList("/test/a", "/test/b")));
    }

    @Test
    public void vectorSimilarityCustomVectorSize() throws Exception {
        final String indexName = "test1";
        final String fieldName1 = "fv1";
        final String fieldName2 = "fv2";
        final String similarityFieldName1 = FieldNames.createSimilarityFieldName(fieldName1);
        final String similarityFieldName2 = FieldNames.createSimilarityFieldName(fieldName2);
        IndexDefinitionBuilder builder = createIndex(fieldName1, fieldName2);
        builder.indexRule("nt:base").property(fieldName1).useInSimilarity(true).nodeScopeIndex()
                .similaritySearchDenseVectorSize(10);
        builder.indexRule("nt:base").property(fieldName2).useInSimilarity(true).nodeScopeIndex()
                .similaritySearchDenseVectorSize(20);
        setIndex(indexName, builder);
        root.commit();
        String alias =  ElasticIndexNameHelper.getIndexAlias(esConnection.getIndexPrefix(), "/oak:index/" + indexName);
        GetFieldMappingsRequest fieldMappingsRequest = new GetFieldMappingsRequest();
        fieldMappingsRequest.indices(alias).fields(similarityFieldName1, similarityFieldName2);
        GetFieldMappingsResponse mappingsResponse = esConnection.getClient().indices().
                getFieldMapping(fieldMappingsRequest, RequestOptions.DEFAULT);
        final Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings =
                mappingsResponse.mappings();
        assertEquals("More than one index found", 1, mappings.keySet().size());
        @SuppressWarnings("unchecked")
        Map<String, Integer> map1 = (Map<String, Integer>)mappings.entrySet().iterator().next().getValue().
                get(similarityFieldName1).sourceAsMap().get(similarityFieldName1);
        assertEquals("Dense vector size doesn't match", 10, map1.get("dims").intValue());
        @SuppressWarnings("unchecked")
        Map<String, Integer> map2 = (Map<String, Integer>)mappings.entrySet().iterator().next().getValue().
                get(similarityFieldName2).sourceAsMap().get(similarityFieldName2);
        assertEquals("Dense vector size doesn't match", 20, map2.get("dims").intValue());
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

        Collection<String> children = new LinkedList<>();
        for (String line : IOUtils.readLines(new FileInputStream(file), Charset.defaultCharset())) {
            String[] split = line.split(",");
            List<Double> values = Arrays.stream(split).skip(1).map(Double::parseDouble).collect(Collectors.toList());
            byte[] bytes = toByteArray(values);
            List<Double> actual = toDoubles(bytes);
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

}
