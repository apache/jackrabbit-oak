/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.ExcerptTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

public class ElasticExcerptTest extends ExcerptTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule(
            ElasticTestUtils.ELASTIC_CONNECTION_STRING);

    public ElasticExcerptTest() {
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

    @Override
    @Test
    @Ignore("no support for excerpt from regexp")
    public void getAllSelectedColumns() throws Exception {
        super.getAllSelectedColumns();
    }

    @Override
    @Test
    @Ignore("no support for excerpt from regexp")
    public void propExcerpt() throws Exception {
        super.propExcerpt();
    }
}