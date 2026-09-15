/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mongot;

import javax.jcr.Repository;
import javax.jcr.Node;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.Row;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexSuggestionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.ClassRule;
import org.junit.Test;

public class MongotIndexSuggestionCommonTest extends IndexSuggestionCommonTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Override
    protected Repository createJcrRepository() {
        indexOptions = new MongotIndexOptions();
        repositoryOptionsUtil = new MongotCommonTestRepositoryBuilder(mongo).build();
        return new Jcr(repositoryOptionsUtil.getOak()).createRepository();
    }

    @Test
    public void explain() throws Exception {
        String nodeType = "nt:unstructured";
        createSuggestIndex("mongodb-explain-suggest", nodeType, "description", true, true);
        Node indexedNode = root.addNode("indexedNode1", nodeType);
        indexedNode.setProperty("description", "foo bar baz");
        session.save();

        QueryManager queryManager = session.getWorkspace().getQueryManager();
        Query query = queryManager.createQuery(
                "EXPLAIN SELECT [rep:suggest()] FROM [" + nodeType + "] WHERE suggest('boo')",
                Query.JCR_SQL2);
        Row row = query.execute().getRows().nextRow();
        String plan = row.getValue("plan").getString();
        MatcherAssert.assertThat(plan, CoreMatchers.containsString(MongoFieldNames.SUGGEST));
        MatcherAssert.assertThat(plan, CoreMatchers.containsString("boo"));
    }
}
