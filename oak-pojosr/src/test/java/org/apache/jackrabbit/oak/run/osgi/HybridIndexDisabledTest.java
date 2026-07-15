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
package org.apache.jackrabbit.oak.run.osgi;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE;

import java.util.LinkedHashMap;
import javax.jcr.Node;
import javax.jcr.Session;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HybridIndexDisabledTest extends AbstractRepositoryFactoryTest {

    @Before
    public void setupRepo() {
        repositoryFactory = new CustomFactory();
        config.put(REPOSITORY_CONFIG_FILE, createConfigValue("oak-base-config.json", "oak-tar-config.json"));
    }

    @After
    public void logout() {
        session.logout();
    }

    @Test
    public void luceneIndexingWithHybridDisabled() throws Exception {
        LinkedHashMap<String, LinkedHashMap<String, Boolean>> map = new LinkedHashMap<>(1);
        LinkedHashMap<String, Boolean> map1 = new LinkedHashMap<String, Boolean>(1);
        map1.put("enableHybridIndexing", false);
        map.put("org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProviderService", map1);
        config.put(REPOSITORY_CONFIG, map);
        repository = repositoryFactory.getRepository(config);

        session = createAdminSession();
        Node fooNode = JcrUtils.getOrCreateByPath("/content/fooNode", "oak:Unstructured", session);
        fooNode.setProperty("foo", "bar");
        session.save();

    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    private Session session;

    private static class CustomFactory extends OakOSGiRepositoryFactory {

        @Override
        protected void preProcessRegistry(PojoServiceRegistry registry) {
            registry.registerService(RepositoryInitializer.class.getName(), new RepositoryInitializer() {
                @Override
                public void initialize(NodeBuilder builder) {
                    if (builder.hasChildNode(INDEX_DEFINITIONS_NAME)) {
                        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);

                        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder();
                        idxBuilder.async("async", "sync");
                        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();
                        index.setChildNode("fooIndex", idxBuilder.build());
                    }
                }
            }, null);
        }
    }
}
