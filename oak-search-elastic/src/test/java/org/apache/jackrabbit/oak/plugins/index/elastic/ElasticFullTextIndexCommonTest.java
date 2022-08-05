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
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.FullTextIndexCommonTest;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.ElasticResultRowAsyncIterator;
import org.junit.ClassRule;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.List;

public class ElasticFullTextIndexCommonTest extends FullTextIndexCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    public ElasticFullTextIndexCommonTest() {
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
        List<String> expectedLogList = new ArrayList<>();
        String log1 = "Error while fetching results from Elastic for [Filter(query=select [jcr:path], [jcr:score]," +
                " * from [nt:base] as a where contains([analyzed_field], 'foo}') /* xpath: //*[jcr:contains(@analyzed_field, 'foo}')]" +
                " */ fullText=analyzed_field:\"foo}\", path=*)]";

        String log2 = "Error while fetching results from Elastic for [Filter(query=select [jcr:path], [jcr:score]," +
                " * from [nt:base] as a where contains([analyzed_field], 'foo]') /* xpath: //*[jcr:contains(@analyzed_field, 'foo]')]" +
                " */ fullText=analyzed_field:\"foo]\", path=*)]";

        expectedLogList.add(log1);
        expectedLogList.add(log2);
        return expectedLogList;
    }
}
