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
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.FullTextAnalyzerCommonTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResultRowAsyncIterator;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.List;

public class ElasticFullTextAnalyzerTest extends FullTextAnalyzerCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

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

        Tree test = root.getTree("/").addChild("test");
        test.setProperty("foo", "die Füchse springen");
        root.commit();

        // standard german analyzer stems verbs (springen -> spring)
        assertEventually(() -> assertQuery("select * from [nt:base] where CONTAINS(*, 'spring')", List.of("/test")));
    }

    @Test(expected = RuntimeException.class)
    public void fulltextSearchWithNotExistentAnalyzerName() throws Exception {
        setup(List.of("foo"), idx -> {
            Tree anl = idx.addChild(FulltextIndexConstants.ANALYZERS).addChild(FulltextIndexConstants.ANL_DEFAULT);
            anl.setProperty(FulltextIndexConstants.ANL_NAME, "this_does_not_exists");
        });
    }

    @Override
    protected String getHinduArabicMapping() {
        return super.getHinduArabicMapping().replaceAll("\"", "");
    }
}
