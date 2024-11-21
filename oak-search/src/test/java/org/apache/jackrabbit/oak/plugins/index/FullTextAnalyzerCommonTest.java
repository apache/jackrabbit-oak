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

import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ANALYZERS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
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

        Tree content = root.getTree("/").addChild("content");
        content.addChild("a").setProperty("analyzed_field", "sun.jpg");
        content.addChild("b").setProperty("analyzed_field", "baz");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(@analyzed_field, 'SUN.JPG')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, 'Sun')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, 'jpg')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(., 'SUN.jpg')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(., 'sun')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(., 'jpg')] ", XPATH, List.of("/content/a"));
        });
    }

    @Test
    public void defaultAnalyzerHonourSplitOptions() throws Exception {
        setup();

        Tree content = root.getTree("/").addChild("content");
        content.addChild("a").setProperty("analyzed_field", "1234abCd5678");
        content.addChild("b").setProperty("analyzed_field", "baz");
        root.commit();

        assertEventually(() -> {
            assertQuery("//*[jcr:contains(@analyzed_field, '1234')] ", XPATH, List.of());
            assertQuery("//*[jcr:contains(@analyzed_field, 'abcd')] ", XPATH, List.of());
            assertQuery("//*[jcr:contains(@analyzed_field, '5678')] ", XPATH, List.of());
            assertQuery("//*[jcr:contains(@analyzed_field, '1234abCd5678')] ", XPATH, List.of("/content/a"));
        });
    }

    @Test
    public void testWithSpecialCharsInSearchTerm() throws Exception {
        setup();

        Tree content = root.getTree("/").addChild("content");
        content.addChild("a").setProperty("analyzed_field", "foo");
        content.addChild("b").setProperty("analyzed_field", "baz");
        root.commit();

        assertEventually(() -> {
            // Special characters {':' , '/', '!', '&', '|', '='} are escaped before creating lucene/elastic queries using
            // {@see org.apache.jackrabbit.oak.plugins.index.search.spi.query.FullTextIndex#rewriteQueryText}
            assertQuery("//*[jcr:contains(@analyzed_field, 'foo:')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '|foo/')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '&=!foo')] ", XPATH, List.of("/content/a"));

            // Braces are not escaped in the above rewriteQueryText method - we do not change that to maintain backward compatibility
            // So these need explicit escaping or filtering on client side while creating the jcr query
            assertQuery("//*[jcr:contains(@analyzed_field, '\\{foo\\}')] ", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@analyzed_field, '\\[foo\\]')] ", XPATH, List.of("/content/a"));
        });

    }

    @Test()
    public void testFullTextTermWithUnescapedBraces() throws Exception {
        LogCustomizer customLogs = setupLogCustomizer();
        setup();

        Tree content = root.getTree("/").addChild("content");
        content.addChild("a").setProperty("analyzed_field", "foo");
        content.addChild("b").setProperty("analyzed_field", "baz");
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

        Tree content = root.getTree("/").addChild("content");
        content.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        content.addChild("b").setProperty("analyzed_field", "bar");
        content.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        content.addChild("d").setProperty("analyzed_field", "baz");

        root.commit();

        assertEventually(() -> {
            assertQuery("//*[j:c/@analyzed_field = 'bar']", XPATH, List.of("/content/a", "/content/c/d"));
            assertQuery("//*[d/*/@analyzed_field = 'bar']", XPATH, List.of("/content/c"));
        });
    }

    @Test
    public void pathTransformationsWithPathRestrictions() throws Exception {
        setup();

        Tree content = root.getTree("/").addChild("content");
        content.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        content.addChild("b").setProperty("analyzed_field", "bar");
        content.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        content.addChild("e").addChild("temp:c").setProperty("analyzed_field", "bar");
        content.addChild("f").addChild("d").addChild("temp:c").setProperty("analyzed_field", "bar");
        content.addChild("g").addChild("e").addChild("temp:c").setProperty("analyzed_field", "bar");
        content.addChild("q").addChild("t").addChild("temp:c").setProperty("analyzed_field", "baz");

        Tree temp = root.getTree("/").addChild("tmp");
        temp.addChild("a").addChild("j:c").setProperty("analyzed_field", "bar");
        temp.getChild("a").setProperty("abc", "foo");
        temp.addChild("b").setProperty("analyzed_field", "bar");
        temp.addChild("c").addChild("d").addChild("j:c").setProperty("analyzed_field", "bar");
        temp.getChild("c").getChild("d").setProperty("abc", "foo");
        root.commit();

        assertEventually(() -> {
            // ALL CHILDREN
            assertQuery("/jcr:root/content//*[j:c/analyzed_field = 'bar']", XPATH, List.of("/content/a", "/content/c/d"));
            assertQuery("/jcr:root/content//*[*/analyzed_field = 'bar']", XPATH, List.of("/content/a", "/content/c/d", "/content/e", "/content/f/d", "/content/g/e"));
            assertQuery("/jcr:root/content//*[d/*/analyzed_field = 'bar']", XPATH, List.of("/content/c", "/content/f"));
            assertQuery("/jcr:root/content//*[analyzed_field = 'bar']", XPATH, List.of("/content/a/j:c", "/content/b", "/content/c/d/j:c",
                    "/content/e/temp:c", "/content/f/d/temp:c", "/content/g/e/temp:c"));

            // DIRECT CHILDREN
            assertQuery("/jcr:root/content/*[j:c/analyzed_field = 'bar']", XPATH, List.of("/content/a"));
            assertQuery("/jcr:root/content/*[*/analyzed_field = 'bar']", XPATH, List.of("/content/a", "/content/e"));
            assertQuery("/jcr:root/content/*[d/*/analyzed_field = 'bar']", XPATH, List.of("/content/c", "/content/f"));
            assertQuery("/jcr:root/content/*[analyzed_field = 'bar']", XPATH, List.of("/content/b"));

            // EXACT
            assertQuery("/jcr:root/content/a[j:c/analyzed_field = 'bar']", XPATH, List.of("/content/a"));
            assertQuery("/jcr:root/content/a[*/analyzed_field = 'bar']", XPATH, List.of("/content/a"));
            assertQuery("/jcr:root/content/c[d/*/analyzed_field = 'bar']", XPATH, List.of("/content/c"));
            assertQuery("/jcr:root/content/a/j:c[analyzed_field = 'bar']", XPATH, List.of("/content/a/j:c"));

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

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "fox jumping");
        content.addChild("baz").setProperty("foo", "dog eating");
        root.commit();

        // standard english analyzer stems verbs (jumping -> jump)
        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'jump')", List.of("/content/bar"));
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

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "dog and cat");
        content.addChild("baz").setProperty("foo", "dog and mouse");
        root.commit();

        // standard english analyzer stems verbs (jumping -> jump)
        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'dog')", List.of());
            assertQuery("select * from [nt:base] where CONTAINS(*, 'cat')", List.of("/content/bar"));
        });
    }

    @Test
    public void fulltextSearchWithCustomComposedFilters() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "whitespace");

            Tree stopFilter = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Stop");
            stopFilter.setProperty("words", "stop1.txt, stop2.txt");
            stopFilter.addChild("stop1.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "foo");
            stopFilter.addChild("stop2.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "and");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "fox jumping");
        content.addChild("baz").setProperty("foo", "dog eating");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'fox foo jumping')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithCustomComposedAnalyzer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree charFilters = anl.addChild(FulltextIndexConstants.ANL_CHAR_FILTERS);
            addFilter(charFilters, "HTMLStrip");
            Tree mappingFilter = addFilter(charFilters, "Mapping");
            mappingFilter.setProperty("mapping", "mappings.txt");
            mappingFilter.addChild("mappings.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, getHinduArabicMapping());

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "LowerCase");
            Tree stopFilter = addFilter(filters, "Stop");
            stopFilter.setProperty("words", "stop1.txt, stop2.txt");
            stopFilter.addChild("stop1.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "my");
            stopFilter.addChild("stop2.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "is");
            addFilter(filters, "PorterStem");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "My license plate is ٢٥٠١٥");
        content.addChild("baz").setProperty("foo", "My license plate is 6789");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, '25015')", List.of("/content/bar")));
    }

    protected String getHinduArabicMapping() {
        // Hindu-Arabic numerals conversion from
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-mapping-charfilter.html
        return "\"٠\" => \"0\"\n\"١\" => \"1\"\n\"٢\" => \"2\"\n\"٣\" => \"3\"\n\"٤\" => \"4\"\n" +
                "\"٥\" => \"5\"\n\"٦\" => \"6\"\n\"٧\" => \"7\"\n\"٨\" => \"8\"\n\"٩\" => \"9\"";
    }

    @Test
    public void fulltextSearchWithCustomComposedAnalyzerWithComments() throws Exception {
        String mappings = new String(getClass().getClassLoader()
                .getResourceAsStream("mapping-ISOLatin1Accent.txt").readAllBytes(), StandardCharsets.UTF_8);
        String stopwords = new String(getClass().getClassLoader()
                .getResourceAsStream("stopwords-snowball.txt").readAllBytes(), StandardCharsets.UTF_8);
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree charFilters = anl.addChild(FulltextIndexConstants.ANL_CHAR_FILTERS);
            Tree mappingFilter = addFilter(charFilters, "Mapping");
            mappingFilter.setProperty("mapping", "mapping-ISOLatin1Accent.txt");
            mappingFilter.addChild("mapping-ISOLatin1Accent.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, mappings);

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree synFilter = addFilter(filters, "Synonym");
            synFilter.setProperty("synonyms", "syn.txt");
            synFilter.setProperty("format", "solr");
            synFilter.setProperty("expand", "true");
            synFilter.setProperty("tokenizerFactory", "standard");
            synFilter.addChild("syn.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "# Synonym mappings can be used for spelling correction too\n" +
                            "tool => instrument");

            addFilter(filters, "LowerCase");
            Tree stopFilter = addFilter(filters, "Stop");
            stopFilter.setProperty("format", "snowball");
            stopFilter.setProperty("enablePositionIncrements", "true");
            stopFilter.setProperty("ignoreCase", "true");
            stopFilter.setProperty("words", "stopwords-snowball.txt");
            stopFilter.addChild("stopwords-snowball.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, stopwords);
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("foobar").setProperty("foo", "IJ");
        content.addChild("baz").setProperty("foo", "B");
        content.addChild("bar").setProperty("foo", "los");
        content.addChild("qux").setProperty("foo", "instrument");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'IJ')", List.of("/content/foobar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'los')", List.of());
            assertQuery("select * from [nt:base] where CONTAINS(*, 'tool')", List.of("/content/qux"));
        });
    }

    @Test
    public void fulltextSearchWithLanguageBasedStemmer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "LowerCase");
            addFilter(filters, "SpanishLightStem");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "torment");
        content.addChild("baz").setProperty("foo", "other text");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'tormenta')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithKStemmer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "KStem");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "the foxes jumping quickly");
        content.addChild("baz").setProperty("foo", "other text");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'quick')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithProtectedStemmer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "LowerCase");
            Tree marker = addFilter(filters, "KeywordMarker");
            marker.setProperty("protected", "protected.txt");
            marker.addChild("protected.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "# some comment here\nrunning");
            addFilter(filters, "PorterStem");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("test").setProperty("foo", "fox running");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'run')", List.of()));
    }

    @Test
    public void fulltextSearchWithPatternReplace() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree charFilters = anl.addChild(FulltextIndexConstants.ANL_CHAR_FILTERS);
            Tree patternReplace = addFilter(charFilters, "PatternReplace");
            patternReplace.setProperty("pattern", "(\\d+)-(?=\\d)");
            patternReplace.setProperty("replacement", "$1");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "My credit card is 123-456-789");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, '123456789')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, '456')", List.of());
        });
    }

    @Test
    public void fulltextSearchWithClassicAnalyzer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Classic");

            addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Classic");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "Q.U.I.C.K.");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'QUICK')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithAsciiFolding() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree asciiFilter = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "AsciiFolding");
            asciiFilter.setProperty("preserveOriginal", "true");
            asciiFilter.setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "açaí");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'acai')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithCJK() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree cjk = addFilter(filters, "CJKBigram");
            cjk.setProperty("hangul", "false");
            cjk.setProperty("hiragana", "false");
            cjk.setProperty("katakana", "false");
            cjk.setProperty("outputUnigrams", "false");
            addFilter(filters, "CJKWidth");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "東京都は、日本の首都であり"); // cjk bigram
        content.addChild("baz").setProperty("foo", "ｼｰｻｲﾄﾞﾗｲﾅｰ"); // cjk width
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, '東京')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'シーサイドライナー')", List.of("/content/baz"));
        });
    }

    @Test
    public void fulltextSearchWithCommonGrams() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree commonGrams = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "CommonGrams");
            commonGrams.setProperty("words", "words.txt");
            commonGrams.addChild("words.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "is\nthe");

        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "the quick fox"); // common grams
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'the_quick')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithDelimitedPayload() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Whitespace");

            Tree delimited = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "DelimitedPayload");
            delimited.setProperty("encoder", "float");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "the|0 brown|10 fox|5 is|0 quick|10");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'brown')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithStemmingAndAsciiFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "LowerCase");
            addFilter(filters, "ASCIIFolding");
            Tree wordDelimiter = addFilter(filters, "WordDelimiter");
            wordDelimiter.setProperty("generateWordParts", "1");
            wordDelimiter.setProperty("stemEnglishPossessive", "1");
            wordDelimiter.setProperty("generateNumberParts", "1");
            wordDelimiter.setProperty("preserveOriginal", "0");
            wordDelimiter.setProperty("splitOnCaseChange", "0");
            wordDelimiter.setProperty("splitOnNumerics", "0");
            wordDelimiter.setProperty("catenateWords", "0");
            wordDelimiter.setProperty("catenateNumbers", "0");
            wordDelimiter.setProperty("catenateAll", "0");
            addFilter(filters, "PorterStem");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "quick");
        content.addChild("baz").setProperty("foo", "quick brown foxes");
        // diacritic form
        content.addChild("bat").setProperty("foo", "maße");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'quick')", List.of("/content/bar", "/content/baz"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'foxes')", List.of("/content/baz"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'fox')", List.of("/content/baz"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'masse')", List.of("/content/bat"));
        });
    }

    @Test
    public void fulltextSearchWithNGram() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Whitespace");

            Tree edgeNGram = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "NGram");
            edgeNGram.setProperty("minGramSize", "2");
            edgeNGram.setProperty("maxGramSize", "3");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "quick");
        content.addChild("baz").setProperty("foo", "kciuq");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'qui')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'ck')", List.of("/content/bar"));
        });
    }

    @Test
    public void fulltextSearchWithEdgeNGram() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Whitespace");

            Tree edgeNGram = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "EdgeNGram");
            edgeNGram.setProperty("minGramSize", "1");
            edgeNGram.setProperty("maxGramSize", "3");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "quick");
        content.addChild("baz").setProperty("foo", "kciuq");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'qui')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithElision() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Whitespace");

            Tree elision = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Elision");
            elision.setProperty("articles", "articles.txt");
            elision.addChild("articles.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "j\ns\nc\nt");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "j'examine");
        content.addChild("baz").setProperty("foo", "other content");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'examine')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithKeepWord() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree kw = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "KeepWord");
            kw.setProperty("words", "words.txt");
            kw.addChild("words.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "dog\nelephant\nfox");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "the quick fox jumps over the lazy dog");
        content.addChild("baz").setProperty("foo", "some other content");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'dog')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'lazy')", List.of());
            assertQuery("select * from [nt:base] where CONTAINS(*, 'content')", List.of());
        });
    }

    @Test
    public void fulltextSearchWithLength() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Whitespace");

            Tree length = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Length");
            length.setProperty("min", "0");
            length.setProperty("max", "4");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "the quick brown fox jumps over the lazy dog");
        content.addChild("baz").setProperty("foo", "more content");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'fox')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'brown')", List.of());
        });
    }

    @Test
    public void fulltextSearchWithLimitTokenCount() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Whitespace");

            Tree length = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "LimitTokenCount");
            length.setProperty("maxTokenCount", "2");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "quick brown fox jumps over the lazy dog");
        content.addChild("baz").setProperty("foo", "more content");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'brown')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'fox')", List.of());
        });
    }

    @Test
    public void fulltextSearchWithLanguageBasedNormalization() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "GermanNormalization");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("test").setProperty("foo", "über");
        content.addChild("baz").setProperty("foo", "other text");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'uber')", List.of("/content/test")));
    }

    @Test
    public void fulltextSearchWithPatternCapture() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree pcg = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "PatternCaptureGroup");
            pcg.setProperty("pattern", "(([a-z]+)(\\d*))");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "abc123def456");
        content.addChild("baz").setProperty("foo", "other text");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'def')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithShingle() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree shingle = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Shingle");
            shingle.setProperty("minShingleSize", "2");
            shingle.setProperty("maxShingleSize", "3");
            shingle.setProperty("outputUnigrams", "false");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "quick brown fox jumps");
        content.addChild("baz").setProperty("foo", "other text");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'quick brown')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithDictionaryCompounderFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree dd = addFilter(filters, "DictionaryCompoundWord");
            dd.setProperty("dictionary", "words.txt");
            dd.addChild("words.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "Donau\ndampf\nmeer\nschiff");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "Donaudampfschiff");
        content.addChild("baz").setProperty("foo", "some other content");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'dampf')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'damp')", List.of());
        });
    }

    //OAK-4805
    @Test
    public void badIndexDefinitionShouldLetQEWork() throws Exception {
        setup(List.of("foo"), idx -> {
            //This would allow index def to get committed. Else bad index def can't be created.
            idx.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, "async");
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree synFilter = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Synonym");
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
    public void fullTextSearchWithTypeTokenFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree type = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Type");
            type.setProperty("types", "stopTypes.txt");
            type.addChild("stopTypes.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "<NUM>\n<SYNONYM>");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "foo 123");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'foo')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, '123')", List.of());
        });
    }

    @Test
    public void fullTextSearchWithWhitelistedTypeTokenFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree type = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Type");
            type.setProperty("types", "stopTypes.txt");
            type.setProperty("useWhitelist", "true");
            type.addChild("stopTypes.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "<NUM>\n<SYNONYM>");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "foo 123");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'foo')", List.of());
            assertQuery("select * from [nt:base] where CONTAINS(*, '123')", List.of("/content/bar"));
        });
    }

    @Test
    public void fullTextWithHunspell() throws Exception {
        String affix = new String(getClass().getClassLoader()
                .getResourceAsStream("fr-affix.txt").readAllBytes(), StandardCharsets.UTF_8);
        String dictionary = new String(getClass().getClassLoader()
                .getResourceAsStream("fr-dictionary.txt").readAllBytes(), StandardCharsets.UTF_8);
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree hunspellStem = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "HunspellStem");
            hunspellStem.setProperty("affix", "fr.aff");
            hunspellStem.addChild("fr.aff").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, affix);
            hunspellStem.setProperty("dictionary", "fr.dic");
            hunspellStem.addChild("fr.dic").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, dictionary);
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "mangé");
        content.addChild("baz").setProperty("foo", "chevaux");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'manger')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'cheval')", List.of("/content/baz"));
        });
    }

    /**
     * This test verifies that the FrenchLightStemmer, an algorithmic stemmer,
     * produces the same results as the dictionary-based Hunspell stemmer (see FullTextAnalyzerCommonsTest#fullTextWithHunspell)
     * for the given French words.
     */
    @Test
    public void fullTextWithFrenchLightStemmer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "LowerCase");
            Tree elision = addFilter(filters, "Elision");
            elision.setProperty("articles", "articles.txt");
            elision.addChild("articles.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "l\nm\nt\nqu\nn\ns\nj\nd\nc\njusqu\nquoiqu\nlorsqu\npuisqu");
            addFilter(filters, "FrenchLightStem");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "mangé");
        content.addChild("baz").setProperty("foo", "chevaux");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'manger')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'cheval')", List.of("/content/baz"));
        });
    }

    @Test
    public void synonyms() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree synFilter = addFilter(anl.addChild(FulltextIndexConstants.ANL_FILTERS), "Synonym");
            synFilter.setProperty("synonyms", "syn.txt");
            synFilter.addChild("syn.txt").addChild(JCR_CONTENT)
                    .setProperty(JCR_DATA, "plane, airplane, aircraft\nflies=>scars");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "an aircraft flies");
        content.addChild("baz").setProperty("foo", "a pen is on the table");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'plane')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'airplane')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'aircraft')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where ISDESCENDANTNODE('/content') and CONTAINS(*, 'scars')", List.of("/content/bar"));
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

        Tree content = root.getTree("/").addChild("content");
        Tree node1Tree = content.addChild("node1");
        node1Tree.setProperty("propa", "abcdef");
        node1Tree.setProperty("propb", "abcdef");
        Tree node2Tree = content.addChild("node2");
        node2Tree.setProperty("propa", "abc_def");
        node2Tree.setProperty("propb", "abc_def");
        Tree node3Tree = content.addChild("node3");
        node3Tree.setProperty("propa", "baz");
        node3Tree.setProperty("propb", "foo");
        root.commit();

        String fullIndexName = indexOptions.getIndexType() + ":" + index.getName();

        assertEventually(() -> {
            //normal query still works
            String query = "select [jcr:path] from [nt:base] where contains('propa', 'abc*')";
            String explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/content/node1", "/content/node2"));

            //unanalyzed wild-card query can still match original term
            query = "select [jcr:path] from [nt:base] where contains('propa', 'abc_d*')";
            explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/content/node2"));

            //normal query still works
            query = "select [jcr:path] from [nt:base] where contains(*, 'abc*')";
            explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/content/node1", "/content/node2"));

            //unanalyzed wild-card query can still match original term
            query = "select [jcr:path] from [nt:base] where contains(*, 'abc_d*')";
            explanation = explain(query);
            assertThat(explanation, containsString(fullIndexName));
            assertQuery(query, List.of("/content/node2"));
        });
    }

    protected Tree addFilter(Tree analyzer, String filterName) {
        Tree filter = analyzer.addChild(filterName);
        // mimics nodes api
        filter.setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);
        try {
            filter.setProperty("binary", root.createBlob(new ByteArrayInputStream(new byte[0])), Type.BINARY);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        filter.setProperty("array", List.of("a", "b"), Type.STRINGS);
        return filter;
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
        builder.includedPaths("/content", "/tmp");
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
