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
        return LogCustomizer.forLogger(ElasticResultRowAsyncIterator.class.getName()).enable(Level.ERROR).create();
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
    public void fulltextSearchWithSnowball() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.addChild(FulltextIndexConstants.ANL_TOKENIZER).setProperty(FulltextIndexConstants.ANL_NAME, "Standard");

            Tree filters = anl.addChild(FulltextIndexConstants.ANL_FILTERS);
            Tree snowball = addFilter(filters, "SnowballPorter");
            snowball.setProperty("language", "Italian");
        });

        Tree content = root.getTree("/").addChild("content");
        content.addChild("bar").setProperty("foo", "mangio la mela");
        content.addChild("baz").setProperty("foo", "altro testo");
        root.commit();

        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'mangiare')", List.of("/content/bar")));
    }

    @Test
    @Ignore("not supported in elasticsearch since hunspell resources need to be available on the server")
    @Override
    public void fullTextWithHunspell() {}
}
