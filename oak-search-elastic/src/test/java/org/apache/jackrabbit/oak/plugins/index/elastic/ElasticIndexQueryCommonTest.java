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
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.IndexQueryCommonTest;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.junit.ClassRule;

public class ElasticIndexQueryCommonTest extends IndexQueryCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

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
        return "[nt:base] as [nt:base] /* elasticsearch:test-index(/oak:index/test-index) "
                + "{\"bool\":{\"filter\":[{\"term\":{\":ancestors\":{\"value\":\"/test\"}}}]}}"
                + " where isdescendantnode([nt:base], [/test]) */";
    }

}
