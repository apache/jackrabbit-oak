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
import org.apache.jackrabbit.oak.plugins.index.PropertyIndexCommonTest;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;

public class ElasticPropertyIndexCommonTest extends PropertyIndexCommonTest {

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static String elasticConnectionString = System.getProperty("elasticConnectionString");
    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    public ElasticPropertyIndexCommonTest() {
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

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        elasticRule.closeElasticConnection();
    }
}
