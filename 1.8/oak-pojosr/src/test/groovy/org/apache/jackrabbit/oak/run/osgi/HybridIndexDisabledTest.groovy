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

package org.apache.jackrabbit.oak.run.osgi

import org.apache.felix.connect.launch.PojoServiceRegistry
import org.apache.jackrabbit.commons.JcrUtils
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer
import org.apache.jackrabbit.oak.spi.state.NodeBuilder
import org.junit.After
import org.junit.Before
import org.junit.Test

import javax.jcr.Node
import javax.jcr.Session

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE


class HybridIndexDisabledTest extends AbstractRepositoryFactoryTest {
    Session session

    @Before
    void setupRepo() {
        repositoryFactory = new CustomFactory()
        config[REPOSITORY_CONFIG_FILE] = createConfigValue("oak-base-config.json", "oak-tar-config.json")
    }

    @After
    void logout() {
        session.logout()
    }

    @Test
    void luceneIndexingWithHybridDisabled() throws Exception{
        config[REPOSITORY_CONFIG] = [
                'org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProviderService' : [
                        'enableHybridIndexing' : false
                ]
        ]
        repository = repositoryFactory.getRepository(config)

        session = createAdminSession()
        Node fooNode = JcrUtils.getOrCreateByPath("/content/fooNode", "oak:Unstructured", session)
        fooNode.setProperty("foo", "bar")
        session.save()

    }

    private static class CustomFactory extends OakOSGiRepositoryFactory {
        @Override
        protected void preProcessRegistry(PojoServiceRegistry registry) {
            registry.registerService(RepositoryInitializer.class.name, new RepositoryInitializer() {
                @Override
                void initialize(NodeBuilder builder) {
                    if (builder.hasChildNode(INDEX_DEFINITIONS_NAME)) {
                        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);

                        IndexDefinitionBuilder idxBuilder = new IndexDefinitionBuilder();
                        idxBuilder.async("async", "sync")
                        idxBuilder.indexRule("nt:base").property("foo").propertyIndex()
                        index.setChildNode("fooIndex", idxBuilder.build())
                    }
                }
            }, null);
        }
    }
}
