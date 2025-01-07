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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.IndexAggregationCommonTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class ElasticIndexAggregationTest extends IndexAggregationCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticIndexAggregationTest() {
        this.indexOptions = new ElasticIndexOptions();
        this.repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
    }

    @Override
    protected ContentRepository createRepository() {
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    @Test
    @Ignore("OAK-10599: Elasticsearch does not support compatibility mode")
    public void oak3371AggregateV1() throws CommitFailedException {
        super.oak3371AggregateV1();
    }
}
