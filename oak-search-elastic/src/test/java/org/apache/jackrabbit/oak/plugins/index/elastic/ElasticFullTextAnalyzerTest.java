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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.FullTextAnalyzerCommonTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResultRowAsyncIterator;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.List;

public class ElasticFullTextAnalyzerTest extends FullTextAnalyzerCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticFullTextAnalyzerTest() {
        this.indexOptions = new ElasticIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    @Override
    protected LogCustomizer setupLogCustomizer() {
        return LogCustomizer.forLogger(ElasticResultRowAsyncIterator.class).enable(Level.ERROR).create();
    }

    @Override
    protected List<String> getExpectedLogMessage() {
        String log1 = "Error while fetching results from Elastic for [Filter(query=select [jcr:path], [jcr:score]," +
                " * from [nt:base] as a where contains([analyzed_field], 'foo}') /* xpath: //*[jcr:contains(@analyzed_field, 'foo}')]" +
                " */ fullText=analyzed_field:\"foo}\", path=*)]";

        String log2 = "Error while fetching results from Elastic for [Filter(query=select [jcr:path], [jcr:score]," +
                " * from [nt:base] as a where contains([analyzed_field], 'foo]') /* xpath: //*[jcr:contains(@analyzed_field, 'foo]')]" +
                " */ fullText=analyzed_field:\"foo]\", path=*)]";

        return List.of(log1, log2);
    }

    @Test
    /*
     * analyzers by name are not possible in lucene, this test can run on elastic only
     */
    public void fulltextSearchWithBuiltInAnalyzerName() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.setProperty(FulltextIndexConstants.ANL_NAME, "german");
        });

        Tree content = root.getTree("/").addChild("content");
        content.setProperty("foo", "die Füchse springen");
        root.commit();

        // standard german analyzer stems verbs (springen -> spring)
        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'spring')", List.of("/content")));
    }

    @Test(expected = RuntimeException.class)
    public void fulltextSearchWithNotExistentAnalyzerName() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.setProperty(FulltextIndexConstants.ANL_NAME, "this_does_not_exists");
        });
    }

    @Test
    /*
     * elastic supports advanced stemmer languages, not available in lucene
     */
    public void fulltextSearchWithAdvancedLanguageBasedStemmer() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "LowerCase");
            Tree stemmer = addFilter(filters, "stemmer");
            stemmer.setProperty("language", "dutch_kp");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "edele");
        content.addChild("baz").setProperty("foo", "other text");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'edeel')", List.of("/content/bar")));
    }

    // these filters are only available in elastic

    @Test
    public void fulltextSearchWithApostropheFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            addFilter(filters, "Apostrophe");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "oak's");
        content.addChild("baz").setProperty("foo", "some other content");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'oak')", List.of("/content/bar")));
    }

    @Test
    public void fulltextSearchWithDictionaryDecompounderFilter() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree dd = addFilter(filters, "dictionary_decompounder");
            dd.setProperty("word_list", "words.txt");
            dd.addChild("words.txt").addChild(JcrConstants.JCR_CONTENT)
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
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree dd = addFilter(filters, "fingerprint");
            dd.setProperty("max_output_size", "10");
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
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree kt = addFilter(filters, "keep_types");
            kt.setProperty("types", "<NUM>");
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
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree mh = addFilter(filters, "min_hash");
            mh.setProperty("hash_count", "1");
            mh.setProperty("bucket_count", "512");
            mh.setProperty("hash_set_size", "1");
            mh.setProperty("with_rotation", "true");
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

    @Test
    @Ignore("not supported in elasticsearch since hunspell resources need to be available on the server")
    @Override
    public void fullTextWithHunspell() {}

    // OAK-12360: a property's own analyzer reference must apply at real index/query
    // time (not just to the in-memory Analyzer object), while a sibling property
    // without one keeps using the index's default analyzer.
    @Test
    public void perPropertyAnalyzerAppliesOnlyToDeclaredProperty() throws Exception {
        setup(List.of("title", "body"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild("titleAnalyzer");
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "whitespace");

            idx.getChild(FulltextIndexConstants.INDEX_RULES).getChild("nt:base")
                    .getChild(FulltextIndexConstants.PROP_NODE).getChild("title")
                    .setProperty(FulltextIndexConstants.PROP_ANALYZER, "titleAnalyzer");
        });

        Tree content = root.getTree("/").addChild("content");
        Tree a = content.addChild("a");
        a.setProperty("title", "Hello World");
        a.setProperty("body", "Hello World");
        root.commit();

        assertEventually(() -> {
            // "title" uses the whitespace tokenizer (no lower-casing): case-sensitive match.
            assertQuery("//*[jcr:contains(@title, 'Hello')]", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@title, 'hello')]", XPATH, List.of());
            // "body" keeps the index's default (lower-casing) analyzer: case-insensitive match.
            assertQuery("//*[jcr:contains(@body, 'hello')]", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@body, 'Hello')]", XPATH, List.of("/content/a"));
        });
    }

    // OAK-12360: a property's analyzer reference that doesn't resolve to any node under
    // analyzers/ must not fail index creation - it falls back to the default analyzer and
    // logs a warning.
    @Test
    public void danglingAnalyzerReferenceFallsBackToDefaultWithWarning() throws Exception {
        LogCustomizer customLogs = LogCustomizer
                .forLogger("org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexHelper")
                .enable(Level.WARN).create();
        customLogs.starting();

        try {
            setup(List.of("title", "body"), idx ->
                    idx.getChild(FulltextIndexConstants.INDEX_RULES).getChild("nt:base")
                            .getChild(FulltextIndexConstants.PROP_NODE).getChild("title")
                            .setProperty(FulltextIndexConstants.PROP_ANALYZER, "doesNotExist"));

            Tree content = root.getTree("/").addChild("content");
            Tree a = content.addChild("a");
            a.setProperty("title", "Hello World");
            root.commit();

            assertEventually(() ->
                    // falls back to the default (lower-casing) analyzer, so this still matches
                    assertQuery("//*[jcr:contains(@title, 'hello')]", XPATH, List.of("/content/a")));

            List<String> logs = customLogs.getLogs();
            Assert.assertTrue("Expected a warning about the unresolved analyzer reference. Captured logs: " + logs,
                    logs.stream().anyMatch(m -> m.contains("doesNotExist")));
        } finally {
            customLogs.finished();
        }
    }

    // OAK-12360: regular-expression property definitions have no single fixed field name to
    // bind a per-property analyzer to, so declaring one there is not supported - it falls back
    // to the index's default handling and logs a warning (mirrors the Lucene-side limitation).
    @Test
    public void regexpPropertyWithAnalyzerFallsBackWithWarning() throws Exception {
        LogCustomizer customLogs = LogCustomizer
                .forLogger("org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexHelper")
                .enable(Level.WARN).create();
        customLogs.starting();

        try {
            setup(List.of("title"), idx -> {
                Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild("frenchAnalyzer");
                anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "whitespace");

                Tree allStrings = idx.getChild(FulltextIndexConstants.INDEX_RULES).getChild("nt:base")
                        .getChild(FulltextIndexConstants.PROP_NODE).addChild("allStrings");
                allStrings.setProperty(FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
                allStrings.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
                allStrings.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
                allStrings.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
                allStrings.setProperty(FulltextIndexConstants.PROP_ANALYZER, "frenchAnalyzer");
            });

            Tree content = root.getTree("/").addChild("content");
            Tree a = content.addChild("a");
            a.setProperty("randomProp", "Hello World");
            root.commit();

            assertEventually(() ->
                    // regexp properties don't get the per-property analyzer wired in - the index still
                    // builds and queries successfully via the aggregate fulltext field (which always
                    // uses the index's default, case-insensitive analyzer)
                    assertQuery("//*[jcr:contains(., 'hello')]", XPATH, List.of("/content/a")));

            List<String> logs = customLogs.getLogs();
            Assert.assertTrue("Expected a warning about regexp properties not supporting per-field analyzers. Captured logs: " + logs,
                    logs.stream().anyMatch(m -> m.contains("regular-expression")));
        } finally {
            customLogs.finished();
        }
    }

    // OAK-12360: a property that explicitly references the literal "default" analyzer node
    // (analyzers/default) must resolve to the same ES analyzer ("oak_analyzer") that
    // ElasticCustomAnalyzer#buildCustomAnalyzers actually registers that node under - not to a
    // literal ES analyzer named "default" (which is never registered and would make Elasticsearch
    // reject index creation with "analyzer [default] not found").
    @Test
    public void explicitDefaultAnalyzerReferenceResolvesToRegisteredDefault() throws Exception {
        setup(List.of("title", "body"), idx -> {
            // Custom default analyzer: whitespace tokenizer only, no lower-casing - so we can tell
            // whether "title" really ends up using it (same as "body", which has no override).
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "whitespace");

            idx.getChild(FulltextIndexConstants.INDEX_RULES).getChild("nt:base")
                    .getChild(FulltextIndexConstants.PROP_NODE).getChild("title")
                    .setProperty(FulltextIndexConstants.PROP_ANALYZER, FulltextIndexConstants.ANL_DEFAULT);
        });

        Tree content = root.getTree("/").addChild("content");
        Tree a = content.addChild("a");
        a.setProperty("title", "Hello World");
        a.setProperty("body", "Hello World");
        root.commit();

        assertEventually(() -> {
            // "title" (explicit "default" reference) and "body" (no override) must behave
            // identically: both go through the custom, case-preserving default analyzer.
            assertQuery("//*[jcr:contains(@title, 'Hello')]", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@title, 'hello')]", XPATH, List.of());
            assertQuery("//*[jcr:contains(@body, 'Hello')]", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@body, 'hello')]", XPATH, List.of());
        });
    }

    // OAK-12360: unlike Lucene (whose :fulltext field is always analyzed by the single index
    // default, regardless of any property's own override), ES's aggregate FieldNames.FULLTEXT
    // query is expanded at query time to also search each nodeScopeIndex-analyzed property's own
    // field (ElasticRequestHandler#fullTextQuery, via IndexingRule#getNodeScopeAnalyzedProps) -
    // because ElasticDocumentMaker never copies an analyzed property's text into :fulltext itself
    // (isFulltextValuePersistedAtNode). When "title" is the sole such contributor (no sibling
    // property sharing the default analyzer to fall back on), its own custom analyzer legitimately
    // governs jcr:contains(., ...) too: the query term is analyzed the same way "title"'s content
    // was, which is correct - not a leaky version of Lucene's guarantee, a genuinely different one.
    @Test
    public void perPropertyAnalyzerAppliesToAggregatedFulltextFieldWhenSoleContributor() throws Exception {
        setup(List.of("title"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild("titleAnalyzer");
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "whitespace");

            idx.getChild(FulltextIndexConstants.INDEX_RULES).getChild("nt:base")
                    .getChild(FulltextIndexConstants.PROP_NODE).getChild("title")
                    .setProperty(FulltextIndexConstants.PROP_ANALYZER, "titleAnalyzer");
        });

        Tree content = root.getTree("/").addChild("content");
        Tree a = content.addChild("a");
        a.setProperty("title", "Hello World");
        root.commit();

        assertEventually(() -> {
            // Property-specific field: custom (whitespace, non-lower-casing) analyzer, case-sensitive.
            assertQuery("//*[jcr:contains(@title, 'Hello')]", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(@title, 'hello')]", XPATH, List.of());
            // Aggregated query: with "title" as the sole nodeScopeIndex-analyzed contributor, it
            // correctly reflects "title"'s own (case-sensitive) analyzer too.
            assertQuery("//*[jcr:contains(., 'Hello')]", XPATH, List.of("/content/a"));
            assertQuery("//*[jcr:contains(., 'hello')]", XPATH, List.of());
        });
    }
}
