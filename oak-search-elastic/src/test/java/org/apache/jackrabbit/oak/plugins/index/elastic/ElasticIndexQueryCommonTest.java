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

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexQueryCommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jcr.query.Query;

public class ElasticIndexQueryCommonTest extends IndexQueryCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticIndexQueryCommonTest() {
        indexOptions = new ElasticIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        ElasticTestRepositoryBuilder elasticTestRepositoryBuilder = new ElasticTestRepositoryBuilder(elasticRule);
        elasticTestRepositoryBuilder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        repositoryOptionsUtil = elasticTestRepositoryBuilder.build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Test
    public void similarityQueryShouldCorrectlyHandleSimilarityTags() throws CommitFailedException {
        String query = "explain select [jcr:path] from [nt:base] where " +
                "native('lucene', 'mlt?stream.body=/test/a&mlt.fl=:path&mlt.mindf=0&mlt.mintf=0')";

        String explainWithoutSimilarityTags = "{\"_source\":{\"includes\":[\":path\"]},\"query\":{\"bool\":{\"should\":[{\"more_like_this\":{\"boost\":3.0,\"include\":true,\"like\":[{\"fields\":[\":dynamic-boost-ft\"],\"_id\":\"/test/a\"}],\"min_doc_freq\":0,\"min_term_freq\":0}},{\"more_like_this\":{\"include\":true,\"like\":[{\"fields\":[\"*\"],\"_id\":\"/test/a\"}],\"min_doc_freq\":0,\"min_term_freq\":0}}]}},\"size\":10,\"sort\":[{\"_score\":{\"order\":\"desc\"}},{\":path\":{\"order\":\"asc\"}}],\"track_total_hits\":10000}";

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("text", "Hello World");
        test.addChild("b").setProperty("text", "He said Hello and then the world said Hello as well.");
        test.addChild("c").setProperty("text", "He said Hi.");

        indexDefn.setProperty("similarityTagsEnabled", false);
        root.commit();

        // similarity tags disabled, should not be present in the explain output
        assertEventually(getAssertionForExplain(query, Query.JCR_SQL2, explainWithoutSimilarityTags, false));

        indexDefn.setProperty("similarityTagsEnabled", true);
        root.commit();

        // similarity tags enabled, but no similarity tags properties configured, should not be present in the explain output
        assertEventually(getAssertionForExplain(query, Query.JCR_SQL2, explainWithoutSimilarityTags, false));

        String explainWithSimilarityTags = "{\"_source\":{\"includes\":[\":path\"]},\"query\":{\"bool\":{\"should\":[{\"more_like_this\":{\"boost\":3.0,\"include\":true,\"like\":[{\"fields\":[\":dynamic-boost-ft\"],\"_id\":\"/test/a\"}],\"min_doc_freq\":0,\"min_term_freq\":0}},{\"more_like_this\":{\"include\":true,\"like\":[{\"fields\":[\"*\"],\"_id\":\"/test/a\"}],\"min_doc_freq\":0,\"min_term_freq\":0}},{\"more_like_this\":{\"boost\":0.5,\"fields\":[\":simTags\"],\"like\":[{\"_id\":\"/test/a\"}],\"min_doc_freq\":1,\"min_term_freq\":1}}]}},\"size\":10,\"sort\":[{\"_score\":{\"order\":\"desc\"}},{\":path\":{\"order\":\"asc\"}}],\"track_total_hits\":10000}";
        Tree properties = indexDefn.getChild(FulltextIndexConstants.INDEX_RULES).getChild("nt:base").getChild("properties");
        Tree simProp = TestUtil.enableForFullText(properties, "simProp", false);
        simProp.setProperty(FulltextIndexConstants.PROP_SIMILARITY_TAGS, true);
        root.commit();

        assertEventually(getAssertionForExplain(query, Query.JCR_SQL2, explainWithSimilarityTags, false));
    }

    @Override
    public String getContainsValueForEqualityQuery_native() {
        return "\"filter\":[{\"term\":{\":ancestors\":{\"value\":\"/test\"}}},{\"term\":{\"propa.keyword\":{\"value\":\"bar\"}}}]";
    }

    @Override
    public String getContainsValueForInequalityQuery_native() {
        return "\"filter\":[{\"term\":{\":ancestors\":{\"value\":\"/test\"}}},{\"exists\":{\"field\":\"propa\"}}," +
                "{\"bool\":{\"must_not\":[{\"term\":{\"propa.keyword\":{\"value\":\"bar\"}}}]";
    }

    @Override
    public String getContainsValueForInequalityQueryWithoutAncestorFilter_native() {
        return "\"filter\":[{\"exists\":{\"field\":\"propa\"}},{\"bool\":" +
                "{\"must_not\":[{\"term\":{\"propa.keyword\":{\"value\":\"bar\"}}}]";
    }

    @Override
    public String getContainsValueForEqualityInequalityCombined_native() {
        return "\"filter\":[{\"term\":{\":ancestors\":{\"value\":\"/test\"}}},{\"term\":{\"propb.keyword\":{\"value\":\"world\"}}}," +
                "{\"exists\":{\"field\":\"propa\"}},{\"bool\":{\"must_not\":[{\"term\":{\"propa.keyword\":{\"value\":\"bar\"}}}]";
    }

    @Override
    public String getContainsValueForNotNullQuery_native() {
        return "\"filter\":[{\"term\":{\":ancestors\":{\"value\":\"/test\"}}},{\"exists\":{\"field\":\"propa\"}}]";
    }

    @Override
    public String getExplainValueForDescendantTestWithIndexTagExplain() {
        return "{\"_source\":{\"includes\":[\":path\"]},\"query\":{\"bool\":{\"filter\":[{\"term\":{\":ancestors\":{\"value\":\"/test\"}}}]}},\"size\":10,\"sort\":[{\"_score\":{\"order\":\"desc\"}},{\":path\":{\"order\":\"asc\"}}],\"track_total_hits\":10000}";
    }

}
