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

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.jcr.Node;
import javax.jcr.Session;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.junit.Assume;
import org.junit.Test;

public class SecondaryStoreConfigIT extends AbstractRepositoryFactoryTest {

    @Test
    public void secondaryNodeStoreCache() throws Exception {
        mongoCheck();

        MongoUtils.dropDatabase(MongoUtils.DB);
        config.put(REPOSITORY_CONFIG_FILE, createConfigValue("oak-base-config.json"));
        Map<String, Map<String, Object>> map = new LinkedHashMap<>( 3);
        Map<String, Object> map1 = new LinkedHashMap<>(2);
        map1.put("mongouri", MongoUtils.URL);
        map1.put("db", MongoUtils.DB);
        map.put("org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService", map1);
        Map<String, Object> map2 = new LinkedHashMap<>(1);
        map2.put("role", "secondary");
        map.put("org.apache.jackrabbit.oak.segment.SegmentNodeStoreFactory-secondary.config", map2);
        Map<String, Object> map3 = new LinkedHashMap<>(1);
        map3.put("includedPaths", List.of("/"));
        map.put("org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreCacheService", map3);
        config.put(REPOSITORY_CONFIG, map);

        repository = repositoryFactory.getRepository(config);

        Session session = createAdminSession();
        Node rootNode = session.getRootNode();

        Node child = JcrUtils.getOrAddNode(rootNode, "testNode", "oak:Unstructured");
        child.setProperty("foo", "bar");
        session.save();
        session.logout();

        NodeStoreProvider nsp = getServiceWithWait(NodeStoreProvider.class);
        final NodeStore secondary = nsp.getNodeStore();

        //Assert that recently created node gets pushed to Secondary and
        //is accessible in sometime
        AbstractRepositoryFactoryTest.retry(30,
                10,
                () -> NodeStateUtils.getNode(secondary.getRoot(), "/testNode").exists());

    }

    private void mongoCheck() {
        //Check if Mongo is available
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

}
