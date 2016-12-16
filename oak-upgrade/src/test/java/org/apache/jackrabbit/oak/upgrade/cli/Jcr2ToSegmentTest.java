/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade.cli;

import java.io.File;
import java.io.IOException;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentNodeStoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Jcr2ToSegmentTest {

    private final NodeStoreContainer destinationContainer = new SegmentNodeStoreContainer();

    private NodeStore destination;

    private RepositoryImpl repository;

    private Session session;

    public Jcr2ToSegmentTest() throws IOException {
    }

    @Before
    public void prepare() throws Exception {
        File tempDir = new File("target", "test-jcr2");
        if (!tempDir.isDirectory()) {
            Util.unzip(AbstractOak2OakTest.class.getResourceAsStream("/jcr2.zip"), tempDir);
        }

        OakUpgrade.main("--copy-binaries", tempDir.getPath(), destinationContainer.getDescription());

        destination = destinationContainer.open();
        repository = (RepositoryImpl) new Jcr(destination).with("oak.sling").with(new ReferenceIndexProvider()).createRepository();
        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    @After
    public void clean() throws IOException {
        session.logout();
        repository.shutdown();
        destinationContainer.close();
        destinationContainer.clean();
    }

    @Test
    public void validateMigration() throws RepositoryException, IOException {
        AbstractOak2OakTest.verifyContent(session);
        AbstractOak2OakTest.verifyBlob(session);
    }

}
