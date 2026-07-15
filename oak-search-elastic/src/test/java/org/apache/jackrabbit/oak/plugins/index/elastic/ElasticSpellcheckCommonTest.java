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
import org.apache.jackrabbit.oak.plugins.index.SpellcheckCommonTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.Row;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;

public class ElasticSpellcheckCommonTest extends SpellcheckCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    @After
    public void cleanup() {
        anonymousSession.logout();
        adminSession.logout();
    }

    protected Repository createJcrRepository() {
        indexOptions = new ElasticIndexOptions();
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        return jcr.createRepository();
    }

    @Test
    public void explain() throws Exception {
        QueryManager qm = adminSession.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", adminSession));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "descent");
        adminSession.save();

        String sql = "EXPLAIN SELECT [rep:spellcheck()] FROM [nt:base] WHERE SPELLCHECK('desent')";
        String expected = "{\"suggest\":{\"oak:suggestion\":{\"phrase\":{\"field\":\":spellcheck\",\"size\":10,\"collate\":" +
                "{\"query\":{\"source\":\"{\\\"bool\\\":{\\\"must\\\":[{\\\"match_phrase\\\":{\\\":spellcheck\\\":{\\\"query\\\":\\\"{{suggestion}}\\\"}}}]}}\"}}," +
                "\"direct_generator\":[{\"field\":\":spellcheck\",\"size\":10,\"suggest_mode\":\"missing\"}]}},\"text\":\"desent\"}}";

        Query q = qm.createQuery(sql, Query.JCR_SQL2);
        Row row = q.execute().getRows().nextRow();
        MatcherAssert.assertThat(row.getValue("plan").getString(), CoreMatchers.containsString(expected));
    }
}
