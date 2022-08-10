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

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.plugins.index.SameQueryResultsWithAndWithoutIndexTest;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.junit.ClassRule;

public class ElasticSameQueryResultsWithAndWithoutIndexTest extends SameQueryResultsWithAndWithoutIndexTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    public ElasticSameQueryResultsWithAndWithoutIndexTest() {
        indexOptions = new ElasticIndexOptions();
        super.passingQueries = ImmutableList.of(
                // Full-text queries
                "/jcr:root//*[jcr:contains(@propa, '*')]",
                "/jcr:root//*[jcr:contains(@propa, '123*')]",
                "/jcr:root//*[jcr:contains(@propa, 'fal*')]"
        );

        super.failingQueries = ImmutableList.of(
                "/jcr:root//*[@propa]",
                "/jcr:root//*[@propa > 0]",
                "/jcr:root//*[@propa > '0']",
                "/jcr:root//*[@propa = 1.11]",
                "/jcr:root//*[@propa = '1.11']",
                "/jcr:root//*[@propa > 1]",
                "/jcr:root//*[@propa > '1']",
                "/jcr:root//*[@propa > 1111]",
                "/jcr:root//*[@propa > '1111']",
                "/jcr:root//*[@propa = true]",
                "/jcr:root//*[@propa = 'true']",
                "/jcr:root//*[@propa = false]",
                "/jcr:root//*[@propa = 'false']"
        );
    }

    @Override
    protected ContentRepository createRepository() {
        ElasticTestRepositoryBuilder elasticTestRepositoryBuilder = new ElasticTestRepositoryBuilder(elasticRule);
        elasticTestRepositoryBuilder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        elasticTestRepositoryBuilder.setAsync(true);
        repositoryOptionsUtil = elasticTestRepositoryBuilder.build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }
}
