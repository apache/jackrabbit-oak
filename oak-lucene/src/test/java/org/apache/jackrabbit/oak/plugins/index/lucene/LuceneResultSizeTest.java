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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.index.ResultSizeCommonTest;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.Node;
import javax.jcr.Repository;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.jackrabbit.JcrConstants.NT_FILE;

public class LuceneResultSizeTest extends ResultSizeCommonTest {

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Override
    protected Repository createJcrRepository() {
        indexOptions = new LuceneIndexOptions();
        repositoryOptionsUtil = new LuceneTestRepositoryBuilderWithAggregator(
                executorService, temporaryFolder).build();
        Oak oak = repositoryOptionsUtil.getOak();
        return new Jcr(oak).withFastQueryResultSize(true).createRepository();
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
    }

    /**
     * Lucene V1 index format uses the AggregateIndex query path when a NodeAggregator is
     * registered. AggregateIndex evaluates "Hello World" as FullTextAnd(Hello, World) and
     * creates a separate index sub-plan per term; IntersectionCursor.getSize() sums the two
     * per-term sizes, so the conjunction query reports ~2*NODE_COUNT instead of ~NODE_COUNT.
     * Lucene-format specific: no Elasticsearch equivalent.
     */
    @Test
    public void testResultSizeLuceneV1() throws Exception {
        Node oakIndex = adminSession.getRootNode().getNode("oak:index");
        // disable the V2 index set up by createIndex() so only the V1 index is used
        oakIndex.getNode(indexName).setProperty("type", "disabled");
        // A bare V1 lucene index (no indexRules) triggers IndexDefinition's auto-rule
        // creation path which produces an allProps rule with analyzed=true,
        // making isFullTextEnabled()=true so LuceneIndex/AggregateIndex handle the query.
        Node luceneV1 = oakIndex.addNode("luceneV1", "oak:QueryIndexDefinition");
        luceneV1.setProperty("type", "lucene");
        luceneV1.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V1.getVersion());
        luceneV1.setProperty("reindex", true);
        adminSession.save();

        try {
            assertEventually(() -> {
                try {
                    // V1 aggregates term hits at query time: a conjunction "Hello World" is
                    // parsed as FullTextAnd(Hello, World), each term is estimated independently
                    // and sizes summed by IntersectionCursor → ~2*NODE_COUNT
                    doTestResultSize(false, 2 * NODE_COUNT);
                    doTestResultSize(true, 2 * NODE_COUNT);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            luceneV1.remove();
            oakIndex.getNode(indexName).setProperty("type", "lucene");
            oakIndex.getNode(indexName).setProperty("reindex", true);
            adminSession.save();
        }
    }

    /**
     * Extends LuceneTestRepositoryBuilder to register a NodeAggregator on the
     * LuceneIndexProvider. The aggregator's specific rule (nt:file/jcr:content) is
     * irrelevant to the test data; its mere presence enables AggregateIndex's composite-plan
     * (per-term) path, which is required for testResultSizeLuceneV1 to report ~2*NODE_COUNT.
     * It does not affect testResultSize because the V2 index (used by that test) is not
     * handled by LuceneIndex, so AggregateIndex produces no plan for V2 queries.
     */
    private static class LuceneTestRepositoryBuilderWithAggregator extends LuceneTestRepositoryBuilder {
        LuceneTestRepositoryBuilderWithAggregator(ExecutorService executorService,
                TemporaryFolder temporaryFolder) {
            super(executorService, temporaryFolder);
            // indexProvider is the LuceneIndexProvider (protected field from TestRepositoryBuilder).
            // Setting the aggregator here, after super() creates resultCountingIndexProvider,
            // still works because ResultCountingIndexProvider delegates getQueryIndexes() to
            // indexProvider, which reads this.aggregator lazily when building AggregateIndex.
            ((LuceneIndexProvider) indexProvider).with(
                    new SimpleNodeAggregator().newRuleWithName(
                            NT_FILE, List.of("jcr:content")));
        }
    }
}
