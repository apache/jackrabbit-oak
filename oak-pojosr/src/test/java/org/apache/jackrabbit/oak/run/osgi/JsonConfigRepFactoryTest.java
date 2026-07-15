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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class JsonConfigRepFactoryTest extends AbstractRepositoryFactoryTest {

    @Before
    public void setupRepo() {
        config.put(REPOSITORY_CONFIG_FILE, getResource("oak-base-config.json").getAbsolutePath());
    }

    @Test
    public void testRepositoryTar() throws Exception {
        config.put(REPOSITORY_CONFIG,
                Map.of("org.apache.jackrabbit.oak.segment.SegmentNodeStoreService", Collections.emptyMap()));

        repository = repositoryFactory.getRepository(config);
        assertNotNull(repository);
        assertNotNull(getService(NodeStore.class));
        basicCrudTest();
    }

    public void basicCrudTest() throws RepositoryException {
        Session session = createAdminSession();
        Node rootNode = session.getRootNode();

        Node child = JcrUtils.getOrAddNode(rootNode, "child", "oak:Unstructured");
        child.setProperty("foo3", "bar3");
        session.save();
        session.logout();

        session = createAdminSession();
        assertEquals("bar3", session.getProperty("/child/foo3").getString());
        session.logout();
    }

}
