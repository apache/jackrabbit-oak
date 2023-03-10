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
package org.apache.jackrabbit.oak.index;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.junit.After;
import org.junit.ClassRule;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.io.File;
import java.io.IOException;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;

public class ElasticAbstractIndexCommandTest extends AbstractIndexTestCommand {

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static final String elasticConnectionString = System.getProperty("elasticConnectionString");

    protected ElasticConnection esConnection;

    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    @Override
    protected IndexRepositoryFixture getRepositoryFixture(File dir) {
        esConnection = getElasticConnection();
        return new ElasticRepositoryFixture(dir,esConnection);
    }

    @Override
    protected void createIndex(String nodeType, String propName, boolean asyncIndex) throws IOException, RepositoryException {
        ElasticIndexDefinitionBuilder idxBuilder = new ElasticIndexDefinitionBuilder();
        if (!asyncIndex) {
            idxBuilder.noAsync();
        }
        idxBuilder.indexRule(nodeType).property(propName).propertyIndex();

        Session session = fixture.getAdminSession();
        Node fooIndex = getOrCreateByPath(TEST_INDEX_PATH,
                "oak:QueryIndexDefinition", session);

        idxBuilder.build(fooIndex);
        session.save();
        session.logout();
    }

    protected ElasticConnection getElasticConnection() {
        return elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
    }

    @After
    public void tearDown() throws IOException {
        if (esConnection != null) {
            esConnection.getClient().indices().delete(i->i
                    .index(esConnection.getIndexPrefix() + "*"));
        }
    }
}
