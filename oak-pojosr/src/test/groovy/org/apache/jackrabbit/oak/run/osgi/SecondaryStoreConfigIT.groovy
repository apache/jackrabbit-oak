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

import org.apache.jackrabbit.commons.JcrUtils
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.apache.jackrabbit.oak.plugins.document.MongoUtils
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider
import org.junit.Test

import javax.jcr.Session

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE
import static org.junit.Assume.assumeTrue


class SecondaryStoreConfigIT extends AbstractRepositoryFactoryTest{

    @Test
    void secondaryNodeStoreCache() throws Exception{
        mongoCheck()
        MongoUtils.dropDatabase(MongoUtils.DB)
        config[REPOSITORY_CONFIG_FILE] = createConfigValue("oak-base-config.json")
        config[REPOSITORY_CONFIG] = [
                'org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService': [
                        mongouri       : MongoUtils.URL,
                        db             : MongoUtils.DB
                ],
                'org.apache.jackrabbit.oak.segment.SegmentNodeStoreFactory-secondary.config' : [
                        role       : "secondary"
                ],
                'org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreCacheService' : [
                        includedPaths : ['/']
                ]
        ]

        repository = repositoryFactory.getRepository(config);

        Session session = createAdminSession();
        javax.jcr.Node rootNode = session.getRootNode();

        javax.jcr.Node child = JcrUtils.getOrAddNode(rootNode, "testNode", "oak:Unstructured");
        child.setProperty("foo", "bar");
        session.save()
        session.logout();

        NodeStoreProvider nsp = getServiceWithWait(NodeStoreProvider.class)
        NodeStore secondary = nsp.nodeStore

        //Assert that recently created node gets pushed to Secondary and
        //is accessible in sometime
        retry(30, 10) {
            return NodeStateUtils.getNode(secondary.root, "/testNode").exists()
        }

    }

    private mongoCheck() {
        //Somehow in Groovy assumeNotNull cause issue as Groovy probably
        //does away with null array causing a NPE
        assumeTrue(MongoUtils.isAvailable())
    }
}
