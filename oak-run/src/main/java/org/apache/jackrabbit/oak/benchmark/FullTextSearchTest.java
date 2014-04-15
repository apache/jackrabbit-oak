/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JcrCustomizer;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;

public class FullTextSearchTest extends AbstractTest<FullTextSearchTest.TestContext> {

    /**
     * Pattern used to find words and other searchable tokens within the
     * imported Wikipedia pages.
     */
    private static final Pattern WORD_PATTERN =
            Pattern.compile("\\p{LD}{3,}");

    private int maxSampleSize = 100;

    private final WikipediaImport importer;

    private final Set<String> sampleSet = newHashSet();

    private final Random random = new Random(42); //fixed seed

    private int count = 0;

    private int maxRowsToFetch = Integer.getInteger("maxRowsToFetch",100);

    private TestContext defaultContext;

    public FullTextSearchTest(File dump, boolean flat, boolean doReport) {
        this.importer = new WikipediaImport(dump, flat, doReport) {
            @Override
            protected void pageAdded(String title, String text) {
                count++;
                if (count % 1000 == 0
                        && sampleSet.size() < maxSampleSize
                        && text != null) {
                    List<String> words = newArrayList();

                    Matcher matcher = WORD_PATTERN.matcher(text);
                    while (matcher.find()) {
                        words.add(matcher.group());
                    }

                    if (!words.isEmpty()) {
                        sampleSet.add(words.get(words.size() / 2));
                    }
                }
            }
        };
    }

    @Override
    public void beforeSuite() throws Exception {
        random.setSeed(42);
        sampleSet.clear();
        count = 0;

        importer.importWikipedia(loginWriter());
        Thread.sleep(10); // allow some time for the indexer to catch up

        defaultContext = new TestContext();
    }

    @Override
    protected TestContext prepareThreadExecutionContext() {
        return new TestContext();
    }

    @Override
    protected void runTest() throws Exception {
        runTest(defaultContext);
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void runTest(TestContext ec)  throws Exception {
        QueryManager qm = ec.session.getWorkspace().getQueryManager();
        // TODO verify why "order by jcr:score()" accounts for what looks
        // like > 20% of the perf lost in Collections.sort
        for (String word : ec.words) {
            Query q = qm.createQuery("//*[jcr:contains(@text, '" + word + "')] ", Query.XPATH);
            QueryResult r = q.execute();
            RowIterator it = r.getRows();
            for (int rows = 0; it.hasNext() && rows < maxRowsToFetch; rows++) {
                Node n = it.nextRow().getNode();
                ec.hash += n.getProperty("text").getString().hashCode();
                ec.hash += n.getProperty("title").getString().hashCode();
            }
        }
    }

    class TestContext {
        final Session session = loginWriter();
        final String[] words = getRandomWords();
        int hash = 0; // summary variable to prevent JIT compiler tricks
    }

    private String[] getRandomWords() {
        List<String> samples = newArrayList(sampleSet);
        String[] words = new String[100];
        for (int i = 0; i < words.length; i++) {
            words[i] = samples.get(random.nextInt(samples.size()));
        }
        return words;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCustomizer() {
                @Override
                public Jcr customize(Jcr jcr) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    jcr.with((QueryIndexProvider) provider)
                       .with((Observer) provider)
                       .with(new LuceneIndexEditorProvider())
                       .with(new LuceneInitializerHelper("luceneGlobal", null));
                    return jcr;
                }
            });
        }
        return super.createRepository(fixture);
    }

}
