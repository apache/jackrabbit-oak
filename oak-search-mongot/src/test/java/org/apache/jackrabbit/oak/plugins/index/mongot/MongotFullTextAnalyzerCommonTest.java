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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.FullTextAnalyzerCommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.mongot.query.MongotIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.event.Level;

public class MongotFullTextAnalyzerCommonTest extends FullTextAnalyzerCommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    public MongotFullTextAnalyzerCommonTest() {
        indexOptions = new MongotIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new MongotCommonTestRepositoryBuilder(mongo).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    @Override
    protected LogCustomizer setupLogCustomizer() {
        return LogCustomizer.forLogger(MongotIndexProvider.class.getName()).enable(Level.ERROR).create();
    }

    @Override
    protected List<String> getExpectedLogMessage() {
        return List.of();
    }

    @Override
    protected void assertEventually(Runnable assertion) {
        TestUtil.assertEventually(assertion, 30_000);
    }

    @After
    public void dropSearchFixtures() {
        mongo.useFreshDatabase();
    }

    @Override
    @Test
    public void testFullTextTermWithUnescapedBraces() throws Exception {
        super.testFullTextTermWithUnescapedBraces();
    }

    @Override
    @Test
    public void fulltextSearchWithCustomComposedAnalyzerWithComments() throws Exception {
        super.fulltextSearchWithCustomComposedAnalyzerWithComments();
    }

    @Override
    @Test
    public void fulltextSearchWithLanguageBasedStemmer() throws Exception {
        super.fulltextSearchWithLanguageBasedStemmer();
    }

    @Override
    @Test
    public void fulltextSearchWithProtectedStemmer() throws Exception {
        super.fulltextSearchWithProtectedStemmer();
    }

    @Override
    @Test
    public void fulltextSearchWithPatternReplace() throws Exception {
        super.fulltextSearchWithPatternReplace();
    }

    @Override
    @Test
    public void fulltextSearchWithClassicAnalyzer() throws Exception {
        super.fulltextSearchWithClassicAnalyzer();
    }

    @Override
    @Test
    public void fulltextSearchWithCJK() throws Exception {
        super.fulltextSearchWithCJK();
    }

    @Override
    @Test
    public void fulltextSearchWithCommonGrams() throws Exception {
        super.fulltextSearchWithCommonGrams();
    }

    @Override
    @Test
    public void fulltextSearchWithDelimitedPayload() throws Exception {
        super.fulltextSearchWithDelimitedPayload();
    }

    @Override
    @Test
    public void fulltextSearchWithWordDelimiterFilter() throws Exception {
        super.fulltextSearchWithWordDelimiterFilter();
    }

    @Override
    @Test
    public void fulltextSearchWithStemmingAndAsciiFilter() throws Exception {
        super.fulltextSearchWithStemmingAndAsciiFilter();
    }

    @Override
    @Test
    public void fulltextSearchWithElision() throws Exception {
        super.fulltextSearchWithElision();
    }

    @Override
    @Test
    public void fulltextSearchWithKeepWord() throws Exception {
        super.fulltextSearchWithKeepWord();
    }

    @Override
    @Test
    public void fulltextSearchWithLanguageBasedNormalization() throws Exception {
        super.fulltextSearchWithLanguageBasedNormalization();
    }

    @Override
    @Test
    public void fulltextSearchWithPatternCapture() throws Exception {
        super.fulltextSearchWithPatternCapture();
    }

    @Override
    @Test
    public void fulltextSearchWithDictionaryCompounderFilter() throws Exception {
        super.fulltextSearchWithDictionaryCompounderFilter();
    }

    @Override
    @Test
    public void fullTextSearchWithTypeTokenFilter() throws Exception {
        super.fullTextSearchWithTypeTokenFilter();
    }

    @Override
    @Test
    public void fullTextSearchWithWhitelistedTypeTokenFilter() throws Exception {
        super.fullTextSearchWithWhitelistedTypeTokenFilter();
    }

    @Override
    @Test
    public void fullTextWithHunspell() throws Exception {
        super.fullTextWithHunspell();
    }

    @Override
    @Test
    public void fullTextWithFrenchLightStemmer() throws Exception {
        super.fullTextWithFrenchLightStemmer();
    }

    @Override
    @Test
    public void synonyms() throws Exception {
        super.synonyms();
    }

    @Override
    @Test
    public void analyzerWithWordDelimiterAndSynonyms() throws Exception {
        super.analyzerWithWordDelimiterAndSynonyms();
    }

    @Override
    @Test
    public void analyzerWithStandardTokenFilter() throws Exception {
        super.analyzerWithStandardTokenFilter();
    }

    @Test
    public void fulltextSearchWithBuiltInAnalyzerName() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.setProperty(FulltextIndexConstants.ANL_NAME, "german");
        });

        root.getTree("/").addChild("content").setProperty("foo", "die Füchse springen");
        root.commit();

        assertEventually(() -> assertQuery(
                "select * from [nt:base] where CONTAINS(*, 'spring')", List.of("/content")));
    }

    @Test(expected = RuntimeException.class)
    public void fulltextSearchWithNotExistentAnalyzerName() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.setProperty(FulltextIndexConstants.ANL_NAME, "this_does_not_exist");
        });
    }

    @Test
    public void fulltextSearchWithAdvancedLanguageBasedStemmer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                    .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree filters = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "LowerCase");
            addFilter(filters, "stemmer").setProperty("language", "dutch_kp");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "edele");
        content.addChild("baz").setProperty("foo", "other text");
        root.commit();

        assertEventually(() -> assertQuery(
                "select * from [nt:base] where CONTAINS(*, 'edeel')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithApostropheFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                    .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            addFilter(analyzer.addChild(FulltextIndexConstants.ANL_FILTERS), "Apostrophe");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "oak's");
        content.addChild("baz").setProperty("foo", "some other content");
        root.commit();

        assertEventually(() -> assertQuery(
                "select * from [nt:base] where CONTAINS(*, 'oak')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithDictionaryDecompounderFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                    .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree decompounder = addFilter(
                    analyzer.addChild(FulltextIndexConstants.ANL_FILTERS), "dictionary_decompounder");
            decompounder.setProperty("word_list", "words.txt");
            decompounder.addChild("words.txt").addChild(JcrConstants.JCR_CONTENT)
                    .setProperty(JcrConstants.JCR_DATA, "Donau\ndampf\nmeer\nschiff");
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

    @Test
    public void fulltextSearchWithFingerprintFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                    .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            addFilter(analyzer.addChild(FulltextIndexConstants.ANL_FILTERS), "fingerprint")
                    .setProperty("max_output_size", "10");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "here here");
        content.addChild("baz").setProperty("foo", "some other quite long content here");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, 'here')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'other')", List.of());
        });
    }

    @Test
    public void fulltextSearchWithKeepTypes() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                    .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            addFilter(analyzer.addChild(FulltextIndexConstants.ANL_FILTERS), "keep_types")
                    .setProperty("types", "<NUM>");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "1 quick fox 2 lazy dogs");
        content.addChild("baz").setProperty("foo", "some other content");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, '2')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'content')", List.of());
        });
    }

    @Test
    public void fulltextSearchWithMinHash() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree analyzer = idx.addChild(FulltextIndexConstants.ANALYZERS)
                    .addChild(FulltextIndexConstants.ANL_DEFAULT);
            analyzer.addChild(FulltextIndexConstants.ANL_TOKENIZER)
                    .setProperty(FulltextIndexConstants.ANL_NAME, "Standard");
            Tree filters = analyzer.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree minHash = addFilter(filters, "min_hash");
            minHash.setProperty("hash_count", "1");
            minHash.setProperty("bucket_count", "512");
            minHash.setProperty("hash_set_size", "1");
            minHash.setProperty("with_rotation", "true");
            Tree shingle = addFilter(filters, "shingle");
            shingle.setProperty("min_shingle_size", "5");
            shingle.setProperty("max_shingle_size", "5");
            shingle.setProperty("output_unigrams", "false");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "1 quick fox 2 lazy dogs");
        content.addChild("baz").setProperty("foo", "some other content");
        root.commit();

        assertEventually(() -> {
            assertQuery("select * from [nt:base] where CONTAINS(*, '2')", List.of("/content/bar"));
            assertQuery("select * from [nt:base] where CONTAINS(*, 'contet')", List.of());
        });
    }
}
