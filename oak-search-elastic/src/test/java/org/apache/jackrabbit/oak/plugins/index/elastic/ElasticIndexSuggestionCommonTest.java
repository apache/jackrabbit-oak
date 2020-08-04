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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.ElasticTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.IndexSuggestionCommonTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtils;
import org.junit.After;
import org.junit.ClassRule;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import java.io.IOException;

public class ElasticIndexSuggestionCommonTest extends IndexSuggestionCommonTest {

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static String elasticConnectionString = System.getProperty("elasticConnectionString");
    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        elasticRule.closeElasticConnection();
    }

    protected Repository createJcrRepository() throws RepositoryException {
        indexOptions = new ElasticIndexOptions();
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();
        return repository;
    }

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT) * 5);
    }
}
