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
import org.apache.jackrabbit.oak.plugins.index.FullTextIndexCommonTest;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticFullTextIndexTest extends FullTextIndexCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticFullTextIndexTest() {
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

    @Test
    public void fullTextWithFuzzyEditDistance() throws Exception {
        Tree index = setup(builder -> builder.indexRule("nt:base").property("propa").analyzed(), idx -> {
                },
                "propa");

        //add content
        Tree test = root.getTree("/").addChild("test");

        test.addChild("a").setProperty("propa", "Hello World!");
        test.addChild("b").setProperty("propa", "Simple test");
        root.commit();

        String misspelledWorld = "//*[jcr:contains(@propa, 'wordl~0.5')]";
        String mixedFuzzyFormats = "//*[jcr:contains(@propa, 'wordl~0.5 OR sample~1')]";

        assertEventually(() -> {
            assertThat(explain(misspelledWorld, XPATH), containsString(indexOptions.getIndexType() + ":" + index.getName()));
            assertQuery(misspelledWorld, XPATH, List.of("/test/a"));
            assertQuery(mixedFuzzyFormats, XPATH, List.of("/test/a", "/test/b"));
        });
    }

}
