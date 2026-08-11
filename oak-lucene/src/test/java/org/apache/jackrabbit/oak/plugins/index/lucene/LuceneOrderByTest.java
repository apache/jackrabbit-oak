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

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.LuceneIndexOptions;
import org.apache.jackrabbit.oak.plugins.index.OrderByCommonTest;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.PropertyType;
import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LuceneOrderByTest extends OrderByCommonTest {

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new LuceneTestRepositoryBuilder(executorService, temporaryFolder).build();
        indexOptions = new LuceneIndexOptions();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Ignore("OAK-7370")
    @Test
    @Override
    public void orderByScoreAcrossUnion() throws Exception {
        super.orderByScoreAcrossUnion();
    }

    @Ignore("multiple DESC conditions do not produce the expected results in lucene")
    @Test
    @Override
    public void orderByMultiProperties() throws Exception {
        super.orderByMultiProperties();
    }

    /**
     * OAK-12343: type=Date on an ordered property requires every value to be a valid date. A value
     * that is not (here "not-a-date") is dropped from the sort field and sorts as the epoch, so
     * ORDER BY returns the wrong order. Lucene-specific (Elastic sorts such values last), so it
     * lives here rather than in OrderByCommonTest. Workaround: declare the property as type=String
     * (see query/lucene.md).
     *
     * TODO(OAK-12344): asserts current Lucene behaviour; update the expected order here if Lucene
     *  is aligned with Elastic.
     */
    @Test
    public void orderByNonDateValuesWithDateTypeIsIncorrect() throws Exception {
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

        // "not-a-date" is dropped from the sort field and sorts as the epoch (first, ascending).
        assertEventually(() -> assertOrderedQuery(
                "select [jcr:path] from [nt:base] where foo = 'bar' order by [dt]",
                List.of("/test/c", "/test/b", "/test/a")));
    }

    /**
     * OAK-12343: worse symptom - no results at all. With type=Date and no path evaluation, ORDER BY
     * is served by a numeric range on the property. The stored values aren't valid ISO-8601 dates,
     * so none can be indexed as a number and the range matches nothing. Lucene-specific (Elastic
     * returns the rows).
     * Workaround: declare the property as type=String (see query/lucene.md).
     *
     * TODO(OAK-12344): asserts current Lucene behaviour; update the expectation here if Lucene is
     *  aligned with Elastic.
     */
    @Test
    public void orderByNonDateStringValuesWithDateTypeReturnsNoResults() throws Exception {
        // no evaluatePathRestrictions, so the numeric range on the date property drives the query
        IndexDefinitionBuilder builder = createIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("dt").propertyIndex().type(PropertyType.TYPENAME_DATE).ordered();
        setIndex(UUID.randomUUID().toString(), createIndex(builder, false, "dt"));

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("dt", "Mon Feb 24 2025 10:22:32 GMT+0000");
        test.addChild("b").setProperty("dt", "Wed Jan 15 2025 09:00:00 GMT+0000");
        test.addChild("c").setProperty("dt", "Fri Mar 07 2025 18:30:00 GMT+0000");
        root.commit();

        // option(traversal fail) forces the index to serve the query, so this asserts index behaviour
        assertEventually(() -> assertOrderedQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test') order by [dt] desc option(traversal fail)",
                List.of()));
    }

}
