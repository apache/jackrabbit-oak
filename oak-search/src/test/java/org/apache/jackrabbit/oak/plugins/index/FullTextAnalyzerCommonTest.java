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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ANALYZERS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class FullTextAnalyzerCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Test
    public void defaultAnalyzer() throws Exception {
        setup();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("analyzed_field", "sun.jpg");
        test.addChild("b").setProperty("analyzed_field", "baz");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(@analyzed_field, 'SUN.JPG')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, 'Sun')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, 'jpg')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(., 'SUN.jpg')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(., 'sun')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(., 'jpg')] ", XPATH, List.of("/test/a"));
        });
    }

    @Test
    public void defaultAnalyzerHonourSplitOptions() throws Exception {
        setup();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("analyzed_field", "1234abCd5678");
        test.addChild("b").setProperty("analyzed_field", "baz");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(@analyzed_field, '1234')] ", XPATH, List.of());
            assertQuery("//*[jcr:contains(@analyzed_field, 'abcd')] ", XPATH, List.of());
            assertQuery("//*[jcr:contains(@analyzed_field, '5678')] ", XPATH, List.of());
            assertQuery("//*[jcr:contains(@analyzed_field, '1234abCd5678')] ", XPATH, List.of("/test/a"));
        });
    }

    @Test
    public void testWithSpecialCharsInSearchTerm() throws Exception {
        setup();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("analyzed_field", "foo");
        test.addChild("b").setProperty("analyzed_field", "baz");
        root.commit();

        assertEventually(() -> {
            // Special characters {':' , '/', '!', '&', '|', '='} are escaped before creating lucene/elastic queries using
            // {@see org.apache.jackrabbit.oak.plugins.index.search.spi.query.FullTextIndex#rewriteQueryText}
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo:')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '|foo/')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '&=!foo')] ", XPATH, List.of("/test/a"));

            // Braces are not escaped in the above rewriteQueryText method - we do not change that to maintain backward compatibility
            // So these need explicit escaping or filtering on client side while creating the jcr query
            assertQuery("//*[jcr:contains(@analyzed_field, '\\{foo\\}')] ", XPATH, List.of("/test/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '\\[foo\\]')] ", XPATH, List.of("/test/a"));
        });

    }

    @Test()
    public void testFullTextTermWithUnescapedBraces() throws Exception {
        LogCustomizer customLogs = setupLogCustomizer();
        setup();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("analyzed_field", "foo");
        test.addChild("b").setProperty("analyzed_field", "baz");
        root.commit();

        // Below queries would fail silently (return 0 results with an entry in logs for the query that failed)
        // due to unescaped special character (which is not handled in backend)
        try {
            customLogs.starting();
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo}')] ", XPATH, List.of());
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo]')] ", XPATH, List.of());

            Assert.assertTrue(customLogs.getLogs().containsAll(getExpectedLogMessage()));
        } finally {
            customLogs.finished();
        }
    }

    @Test
    public void pathTransformationsWithNoPathRestrictions() throws Exception {
        setup();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        test.addChild("b").setProperty("analyzed_field", "bar");
        test.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        test.addChild("d").setProperty("analyzed_field", "baz");

        root.commit();

        assertEventually(() -> {
            assertQuery("//*[j:c/@analyzed_field = 'bar']", XPATH, List.of("/test/a", "/test/c/d"));
            assertQuery("//*[d/*/@analyzed_field = 'bar']", XPATH, List.of("/test/c"));
        });
    }

    @Test
    public void pathTransformationsWithPathRestrictions() throws Exception {
        setup();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        test.addChild("b").setProperty("analyzed_field", "bar");
        test.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        test.addChild("e").addChild("temp:c").setProperty("analyzed_field", "bar");
        test.addChild("f").addChild("d").addChild("temp:c").setProperty("analyzed_field", "bar");
        test.addChild("g").addChild("e").addChild("temp:c").setProperty("analyzed_field", "bar");
        test.addChild("q").addChild("t").addChild("temp:c").setProperty("analyzed_field", "baz");


        Tree temp = root.getTree("/").addChild("tmp");

        temp.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        temp.getChild("a").setProperty("abc", "foo");
        temp.addChild("b").setProperty("analyzed_field", "bar");
        temp.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        temp.getChild("c").getChild("d").setProperty("abc", "foo");
        root.commit();

        assertEventually(() -> {
            // ALL CHILDREN
            assertQuery("/jcr:root/test//*[j:c/analyzed_field = 'bar']", XPATH, List.of("/test/a", "/test/c/d"));
            assertQuery("/jcr:root/test//*[*/analyzed_field = 'bar']", XPATH, List.of("/test/a", "/test/c/d", "/test/e", "/test/f/d", "/test/g/e"));
            assertQuery("/jcr:root/test//*[d/*/analyzed_field = 'bar']", XPATH, List.of("/test/c", "/test/f"));
            assertQuery("/jcr:root/test//*[analyzed_field = 'bar']", XPATH, List.of("/test/a/j:c", "/test/b", "/test/c/d/j:c",
                    "/test/e/temp:c", "/test/f/d/temp:c", "/test/g/e/temp:c"));

            // DIRECT CHILDREN
            assertQuery("/jcr:root/test/*[j:c/analyzed_field = 'bar']", XPATH, List.of("/test/a"));
            assertQuery("/jcr:root/test/*[*/analyzed_field = 'bar']", XPATH, List.of("/test/a", "/test/e"));
            assertQuery("/jcr:root/test/*[d/*/analyzed_field = 'bar']", XPATH, List.of("/test/c", "/test/f"));
            assertQuery("/jcr:root/test/*[analyzed_field = 'bar']", XPATH, List.of("/test/b"));

            // EXACT
            assertQuery("/jcr:root/test/a[j:c/analyzed_field = 'bar']", XPATH, List.of("/test/a"));
            assertQuery("/jcr:root/test/a[*/analyzed_field = 'bar']", XPATH, List.of("/test/a"));
            assertQuery("/jcr:root/test/c[d/*/analyzed_field = 'bar']", XPATH, List.of("/test/c"));
            assertQuery("/jcr:root/test/a/j:c[analyzed_field = 'bar']", XPATH, List.of("/test/a/j:c"));

            // PARENT
            assertQuery("select a.[jcr:path] as [jcr:path] from [nt:base] as a \n" +
                    "  inner join [nt:base] as b on ischildnode(b, a)\n" +
                    "  where isdescendantnode(a, '/tmp') \n" +
                    "  and b.[analyzed_field] = 'bar'\n" +
                    "  and a.[abc] is not null ", SQL2, List.of("/tmp/a", "/tmp/c/d"));
        });
    }

    @Test
    public void fulltextSearchWithBuiltInAnalyzerClass() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.setProperty(FulltextIndexConstants.ANL_CLASS, "org.apache.lucene.analysis.en.EnglishAnalyzer");
        });

        Tree test = root.getTree("/");
        test.addChild("test").setProperty("foo", "fox jumping");
        test.addChild("baz").setProperty("foo", "dog eating");
        root.commit();

        // standard english analyzer stems verbs (jumping -> jump)
        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'jump')", List.of("/test"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'jumpingjack')", List.of());
        });
    }

    @Test(expected = RuntimeException.class)
    public void fulltextSearchWithWrongAnalyzerClass() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.setProperty(FulltextIndexConstants.ANL_CLASS, "org.apache.lucene.analysis.en.BogusAnalyzer");
        });
    }

    @Test
    public void fulltextSearchWithBuiltInAnalyzerClassAndConfigurationParams() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.setProperty(FulltextIndexConstants.ANL_CLASS, "org.apache.lucene.analysis.en.EnglishAnalyzer");
            anl.setProperty("luceneMatchVersion", "LUCENE_47");
            anl.addChild("stopwords").addChild(JCR_CONTENT).setProperty(JCR_DATA, "dog");
        });

        Tree test = root.getTree("/");
        test.addChild("test").setProperty("foo", "dog and cat");
        test.addChild("baz").setProperty("foo", "dog and mouse");
        root.commit();

        // standard english analyzer stems verbs (jumping -> jump)
        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'dog')", List.of());
            assertQuery("select * from [nt:base] where CONTAINS(*, 'cat')", List.of("/test"));
        });
    }

    @Test
    public void fulltextSearchWithCustomComposedFilters() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "whitespace");

            Tree stopFilter = anl.addChild(FulltextIndexConstants.ANL_FILTERS).addChild("Stop");
            stopFilter.setProperty("words", "stop1.txt, stop2.txt");
            stopFilter.addChild("stop1.txt").addChild(JcrConstants.JCR_CONTENT)
                    .setProperty(JcrConstants.JCR_DATA, "foo");
            stopFilter.addChild("stop2.txt").addChild(JcrConstants.JCR_CONTENT)
                    .setProperty(JcrConstants.JCR_DATA, "and");
        });

        Tree test = root.getTree("/");
        test.addChild("test").setProperty("foo", "fox jumping");
        test.addChild("baz").setProperty("foo", "dog eating");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'fox foo jumping')", List.of("/test")));
    }

    @Test
    public void fulltextSearchWithCustomComposedAnalyzer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree charFilters = anl.addChild(FulltextIndexConstants.ANL_CHAR_FILTERS);
            charFilters.addChild("HTMLStrip");
            Tree mappingFilter = charFilters.addChild("Mapping");
            mappingFilter.setProperty("mapping", "mappings.txt");
            mappingFilter.addChild("mappings.txt").addChild(JcrConstants.JCR_CONTENT)
                    .setProperty(JcrConstants.JCR_DATA, getHinduArabicMapping());

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            filters.addChild("LowerCase");
            Tree stopFilter = filters.addChild("Stop");
            stopFilter.setProperty("words", "stop1.txt, stop2.txt");
            stopFilter.addChild("stop1.txt").addChild(JcrConstants.JCR_CONTENT)
                    .setProperty(JcrConstants.JCR_DATA, "my");
            stopFilter.addChild("stop2.txt").addChild(JcrConstants.JCR_CONTENT)
                    .setProperty(JcrConstants.JCR_DATA, "is");
            filters.addChild("PorterStem");
        });

        Tree test = root.getTree("/");
        test.addChild("test").setProperty("foo", "My license plate is ٢٥٠١٥");
        test.addChild("baz").setProperty("foo", "My license plate is 6789");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, '25015')", List.of("/test")));
    }

    protected String getHinduArabicMapping() {
        // Hindu-Arabic numerals conversion from
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-mapping-charfilter.html
        return "\"٠\" => \"0\"\n\"١\" => \"1\"\n\"٢\" => \"2\"\n\"٣\" => \"3\"\n\"٤\" => \"4\"\n" +
                "\"٥\" => \"5\"\n\"٦\" => \"6\"\n\"٧\" => \"7\"\n\"٨\" => \"8\"\n\"٩\" => \"9\"";
    }

    //OAK-4805
    @Test
    public void badIndexDefinitionShouldLetQEWork() throws Exception {
        setup(List.of("foo"), idx -> {
            //This would allow index def to get committed. Else bad index def can't be created.
            idx.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, "async");
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree synFilter = anl.addChild(FulltextIndexConstants.ANL_FILTERS).addChild("Synonym");
            synFilter.setProperty("synonyms", "syn.txt");
            // Don't add syn.txt to make analyzer (and hence index def) invalid
            // synFilter.addChild("syn.txt").addChild(JCR_CONTENT).setProperty(JCR_DATA, "blah, foo, bar");
        });

        //Using this version of executeQuery as we don't want a result row quoting the exception
        assertEventually(() -> {
            try {
                executeQuery("SELECT * FROM [nt:base] where a='b'", SQL2, QueryEngine.NO_BINDINGS);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testSynonyms() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree synFilter = anl.addChild(FulltextIndexConstants.ANL_FILTERS).addChild("Synonym");
            synFilter.setProperty("synonyms", "syn.txt");
            synFilter.addChild("syn.txt").addChild(JcrConstants.JCR_CONTENT)
                    .setProperty(JcrConstants.JCR_DATA, "plane, airplane, aircraft\nflies=>scars");
        });

        Tree test = root.getTree("/").addChild("test");
        test.addChild("node").setProperty("foo", "an aircraft flies");
        test.addChild("baz").setProperty("foo", "a pen is on the table");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'plane')", List.of("/test/node"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'airplane')", List.of("/test/node"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'aircraft')", List.of("/test/node"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/test') and CONTAINS(*, 'scars')", List.of("/test/node"));
        });
    }

    //OAK-4516
    @Test
    public void wildcardQueryToLookupUnanalyzedText() throws Exception {
        Tree index = setup(builder -> {
                    builder.indexRule("nt:base").property("propa").analyzed();
                    builder.indexRule("nt:base").property("propb").nodeScopeIndex();
                }, idx -> idx.addChild(ANALYZERS).setProperty(FulltextIndexConstants.INDEX_ORIGINAL_TERM, true),
                "propa", "propb");

        Tree rootTree = root.getTree("/");
        Tree node1Tree = rootTree.addChild("node1");
        node1Tree.setProperty("propa", "abcdef");
        node1Tree.setProperty("propb", "abcdef");
        Tree node2Tree = rootTree.addChild("node2");
        node2Tree.setProperty("propa", "abc_def");
        node2Tree.setProperty("propb", "abc_def");
        Tree node3Tree = rootTree.addChild("node3");
        node3Tree.setProperty("propa", "baz");
        node3Tree.setProperty("propb", "foo");
        root.commit();

        String fullIndexName = indexOptions.getIndexType() + ":" + index.getName();

        assertEventually(() -> {
            //normal query still works
            String query = "select [jcr:path] from [nt:base] where contains('propa', 'abc*')";
            String explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/node1", "/node2"));

            //unanalyzed wild-card query can still match original term
            query = "select [jcr:path] from [nt:base] where contains('propa', 'abc_d*')";
            explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/node2"));

            //normal query still works
            query = "select [jcr:path] from [nt:base] where contains(*, 'abc*')";
            explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/node1", "/node2"));

            //unanalyzed wild-card query can still match original term
            query = "select [jcr:path] from [nt:base] where contains(*, 'abc_d*')";
            explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/node2"));
        });
    }

    private static final BiConsumer<IndexDefinitionBuilder, List<String>> DEFAULT_BUILDER_HOOK = ((builder, analyzedFields) ->
            analyzedFields.forEach(f -> builder.indexRule("nt:base").property(f).analyzed().nodeScopeIndex()));

    protected Tree setup() throws Exception {
        return setup(List.of("analyzed_field"), idx -> {
        });
    }

    protected Tree setup(List<String> analyzedFields, Consumer<Tree> indexHook) throws Exception {
        return setup(
                builder -> DEFAULT_BUILDER_HOOK.accept(builder, analyzedFields),
                indexHook,
                analyzedFields.toArray(new String[0])
        );
    }

    private Tree setup(Consumer<IndexDefinitionBuilder> builderHook, Consumer<Tree> indexHook, String... propNames) throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), false, propNames);
        builder.noAsync();
        builder.evaluatePathRestrictions();
        builderHook.accept(builder);

        Tree index = indexOptions.setIndex(root, UUID.randomUUID().toString(), builder);
        indexHook.accept(index);
        root.commit();

        return index;
    }

    private String explain(String query) {
        String explain = "explain " + query;
        return executeQuery(explain, "JCR-SQL2").get(0);
    }

    // TODO : Below two method are only used for testFullTextTermWithUnescapedBraces
    // TODO : If needed in future, we can possibly use test metadata to change the
    // TODO : returned values from these based on which test is being executed
    protected abstract LogCustomizer setupLogCustomizer();

    protected abstract List<String> getExpectedLogMessage();

}
