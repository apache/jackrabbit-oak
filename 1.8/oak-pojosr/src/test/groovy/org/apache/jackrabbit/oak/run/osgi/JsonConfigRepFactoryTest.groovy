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
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.junit.Before
import org.junit.Test

import javax.jcr.RepositoryException
import javax.jcr.Session

import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE

class JsonConfigRepFactoryTest extends AbstractRepositoryFactoryTest{

    @Before
    void setupRepo(){
        config[REPOSITORY_CONFIG_FILE] = getResource("oak-base-config.json").absolutePath
    }

    @Test
    public void testRepositoryTar() throws Exception {
        config[REPOSITORY_CONFIG] = [
                'org.apache.jackrabbit.oak.segment.SegmentNodeStoreService' : [:]
        ]

        repository = repositoryFactory.getRepository(config);
        assert repository
        assert getService(NodeStore.class)
        basicCrudTest()
    }

    def basicCrudTest() throws RepositoryException {
        Session session = createAdminSession();
        javax.jcr.Node rootNode = session.getRootNode();

        javax.jcr.Node child = JcrUtils.getOrAddNode(rootNode, "child", "oak:Unstructured");
        child.setProperty("foo3", "bar3");
        session.save()
        session.logout();

        session = createAdminSession();
        assert session.getProperty('/child/foo3').string == 'bar3'
        session.logout()
    }
}