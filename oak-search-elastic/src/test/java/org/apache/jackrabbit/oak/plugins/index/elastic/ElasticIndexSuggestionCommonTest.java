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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexSuggestionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.query.Query;
import javax.jcr.query.Row;

public class ElasticIndexSuggestionCommonTest extends IndexSuggestionCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    protected Repository createJcrRepository() {
        indexOptions = new ElasticIndexOptions();
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        return jcr.createRepository();
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT) * 5);
    }

    @Test
    public void explain() throws Exception {
        String nodeType = "nt:unstructured";
        String indexPropName = "description";

        createSuggestIndex("elastic-explain-suggest", nodeType, "description", true, true);

        Node indexedNode = root.addNode("indexedNode1", nodeType);
        indexedNode.setProperty(indexPropName, "foo bar baz");
        session.save();

        String sql = "EXPLAIN SELECT [rep:suggest()] FROM [" + nodeType + "] WHERE suggest('boo')";
        String expected = "{\"_source\":{\"includes\":[\":path\"]},\"query\":{\"bool\":{\"must\":[{\"nested\":{\"inner_hits\":" +
                "{\"size\":100},\"path\":\":suggest\",\"query\":{\"match_bool_prefix\":{\":suggest.value\":{\"operator\":\"and\",\"query\":\"boo\"}}},\"score_mode\":\"max\"}}]}},\"size\":100}";

        Query q = qm.createQuery(sql, Query.SQL);
        Row row = q.execute().getRows().nextRow();
        MatcherAssert.assertThat(row.getValue("plan").getString(), CoreMatchers.containsString(expected));
    }
}
