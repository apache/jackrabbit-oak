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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

public class ElasticSimilarQueryTest extends ElasticAbstractQueryTest {

    /*
    This test mirror the test org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexQueryTest#testRepSimilarAsNativeQuery
    Exact same test data, to test out for feature parity
    The only difference is the same query in lucene returns the doc itself (the one that we need similar docs of) as part of search results
    whereas in elastic, it doesn't.
     */
    @Test
    public void testRepSimilarAsNativeQuery() throws Exception {

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
    public void testRepSimilarQuery() throws Exception {
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
    public void testRepSimilarXPathQuery() throws Exception {
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
    public void testRepSimilarWithStopWords() throws Exception {
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
    public void testRepSimilarWithMinWordLength() throws Exception {
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
    public void testRepSimilarQueryWithLongPath() throws Exception {
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

    private void createIndex(boolean nativeQuery) throws Exception {
        IndexDefinitionBuilder builder = createIndex("text");
        if (nativeQuery) {
            builder.getBuilderTree().setProperty(FulltextIndexConstants.FUNC_NAME, "elastic-sim");
        }
        builder.indexRule("nt:base").property("text").analyzed();
        String indexId = UUID.randomUUID().toString();
        setIndex(indexId, builder);
        root.commit();
    }

}
