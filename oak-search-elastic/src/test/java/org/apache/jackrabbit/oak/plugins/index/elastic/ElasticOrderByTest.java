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

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.OrderByCommonTest;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jcr.PropertyType;
import java.util.List;
import java.util.UUID;

public class ElasticOrderByTest extends OrderByCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticOrderByTest() {
        indexOptions = new ElasticIndexOptions();
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

    /**
     * OAK-12343: Elastic counterpart to the Lucene type=Date limitation. Elastic sorts missing
     * (non-date) values last instead of as the epoch, so ORDER BY returns the valid dates in order
     * with the non-date value last - it does not mis-order or drop rows the way Lucene does
     * (LuceneOrderByTest#orderByNonDateValuesWithDateTypeIsIncorrect).
     *
     * TODO(OAK-12344): if Lucene is aligned with this, this becomes the shared expectation.
     */
    @Test
    public void orderByNonDateValuesWithDateTypeSortsMissingLast() throws Exception {
        IndexDefinitionBuilder builder = createIndexDefinitionBuilder();
        builder.evaluatePathRestrictions();
        IndexDefinitionBuilder.IndexRule rule = builder.indexRule("nt:base");
        rule.property("foo").propertyIndex();
        rule.property("dt").propertyIndex().type(PropertyType.TYPENAME_DATE).ordered();
        setIndex(UUID.randomUUID().toString(), createIndex(builder, false, "foo", "dt"));

        Tree test = root.getTree("/").addChild("test");
        Tree a = test.addChild("a");
        a.setProperty("foo", "bar");
        a.setProperty("dt", "2023-06-05T16:59:48.119Z");
        Tree b = test.addChild("b");
        b.setProperty("foo", "bar");
        b.setProperty("dt", "2013-06-21T08:30:48.119Z");
        Tree c = test.addChild("c");
        c.setProperty("foo", "bar");
        c.setProperty("dt", "not-a-date");
        root.commit();

        // valid dates order chronologically; the non-date value sorts last (missing=_last).
        assertEventually(() -> assertOrderedQuery(
                "select [jcr:path] from [nt:base] where foo = 'bar' order by [dt]",
                List.of("/test/b", "/test/a", "/test/c")));
    }

}
