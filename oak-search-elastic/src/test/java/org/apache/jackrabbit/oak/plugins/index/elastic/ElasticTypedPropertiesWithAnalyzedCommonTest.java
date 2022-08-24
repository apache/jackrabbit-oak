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
import org.apache.jackrabbit.oak.plugins.index.TypedPropertiesWithAnalyzedCommonTest;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.junit.ClassRule;
import org.junit.Test;

public class ElasticTypedPropertiesWithAnalyzedCommonTest extends TypedPropertiesWithAnalyzedCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule =
            new ElasticConnectionRule(ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    public ElasticTypedPropertiesWithAnalyzedCommonTest() {
        indexOptions = new ElasticIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        ElasticTestRepositoryBuilder elasticTestRepositoryBuilder = new ElasticTestRepositoryBuilder(elasticRule);
        elasticTestRepositoryBuilder.setNodeStore(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT));
        elasticTestRepositoryBuilder.setAsync(true);
        repositoryOptionsUtil = elasticTestRepositoryBuilder.build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Test
    public void typeBooleanAnalyzed() throws Exception {
        super.prepareIndexForBooleanPropertyTest();
        // OAK-9875: fixed the support for full-text queries in analyzed, non-Text fields (e.g., long, analyzed),
        // with the exception of Boolean fields because Elastic lacks support for the ignored_malformed option on
        // Booleans. Therefore, these queries still fail.
        assertQuery("/jcr:root//*[jcr:contains(@propa, '123*')]", XPATH, ImmutableList.of());
        assertQuery("/jcr:root//*[jcr:contains(@propa, '432*')]", XPATH, ImmutableList.of());
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'notpresent*')]", XPATH, ImmutableList.of());
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'Lorem*')]", XPATH, ImmutableList.of());
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'tru*')]", XPATH, ImmutableList.of());

        assertQuery("/jcr:root//*[@propa]", XPATH, ImmutableList.of(
                "/test/p1", "/test/p10", "/test/p11", "/test/p12", "/test/p13", "/test/p2", "/test/p3", "/test/p4", "/test/p5", "/test/p6", "/test/p7", "/test/p8", "/test/p9"
        ));
        assertQuery("/jcr:root//*[@propa = true]", XPATH, ImmutableList.of("/test/p5", "/test/p6"));
        assertQuery("/jcr:root//*[@propa = false]", XPATH, ImmutableList.of("/test/p11", "/test/p12"));
    }
}
