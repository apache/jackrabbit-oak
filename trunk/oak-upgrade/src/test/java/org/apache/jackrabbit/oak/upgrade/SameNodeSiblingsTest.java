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
package org.apache.jackrabbit.oak.upgrade;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SameNodeSiblingsTest {

    public static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private File crx2RepoDir;

    @Before
    public void createCrx2RepoDir() throws IOException {
        crx2RepoDir = Files.createTempDirectory(Paths.get("target"), "repo-crx2").toFile();
    }

    @After
    public void deleteCrx2RepoDir() {
        FileUtils.deleteQuietly(crx2RepoDir);
    }

    @Test
    public void snsShouldBeRenamed() throws RepositoryException, IOException {
        DocumentNodeStore nodeStore = migrate(new SourceDataCreator() {
            @Override
            public void create(Session session) throws RepositoryException {
                Node parent = session.getRootNode().addNode("parent");
                parent.addNode("child", "nt:folder");
                parent.addNode("child", "nt:folder");
                parent.addNode("child", "nt:folder");
                parent.addNode("something_else", "nt:folder");
                session.save();

                parent.setPrimaryType("nt:folder"); // change parent type to
                                                    // something that doesn't
                                                    // allow SNS
                session.save();
            }
        });
        try {
            NodeState parent = nodeStore.getRoot().getChildNode("parent");
            Set<String> children = newHashSet(parent.getChildNodeNames());
            assertEquals(of("child", "child_2_", "child_3_", "something_else"), children);
        } finally {
            nodeStore.dispose();
        }
    }

    @Test
    public void snsShouldntBeRenamed() throws RepositoryException, IOException {
        DocumentNodeStore nodeStore = migrate(new SourceDataCreator() {
            @Override
            public void create(Session session) throws RepositoryException {
                Node parent = session.getRootNode().addNode("parent");
                parent.addNode("child", "nt:folder");
                parent.addNode("child", "nt:folder");
                parent.addNode("child", "nt:folder");
                parent.addNode("something_else", "nt:folder");
                session.save();
            }
        });
        try {
            NodeState parent = nodeStore.getRoot().getChildNode("parent");
            Set<String> children = newHashSet(parent.getChildNodeNames());
            assertEquals(of("child", "child[2]", "child[3]", "something_else"), children);
        } finally {
            nodeStore.dispose();
        }
    }

    @Test
    public void snsNewNameAlreadyExists() throws RepositoryException, IOException {
        DocumentNodeStore nodeStore = migrate(new SourceDataCreator() {
            @Override
            public void create(Session session) throws RepositoryException {
                Node parent = session.getRootNode().addNode("parent");
                parent.addNode("child", "nt:folder");
                parent.addNode("child", "nt:folder");
                parent.addNode("child", "nt:folder");
                parent.addNode("child_2_", "nt:folder");
                parent.addNode("child_3_", "nt:folder");
                parent.addNode("child_3_2", "nt:folder");
                session.save();

                parent.setPrimaryType("nt:folder");
                session.save();
            }
        });
        try {
            NodeState parent = nodeStore.getRoot().getChildNode("parent");
            Set<String> children = newHashSet(parent.getChildNodeNames());
            assertEquals(of("child", "child_2_", "child_3_", "child_2_2", "child_3_2", "child_3_3"), children);
        } finally {
            nodeStore.dispose();
        }
    }

    private DocumentNodeStore migrate(SourceDataCreator sourceDataCreator) throws RepositoryException, IOException {
        RepositoryConfig config = RepositoryConfig.install(crx2RepoDir);
        RepositoryImpl repository = RepositoryImpl.create(config);

        try {
            Session session = repository.login(CREDENTIALS);
            sourceDataCreator.create(session);
            session.logout();
        } finally {
            repository.shutdown();
        }

        config = RepositoryConfig.install(crx2RepoDir); // re-create the config
        RepositoryContext context = RepositoryContext.create(config);
        DocumentNodeStore target = newDocumentNodeStoreBuilder().build();
        try {
            RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.copy(null);
        } finally {
            context.getRepository().shutdown();
        }
        return target;
    }

    private static interface SourceDataCreator {
        void create(Session session) throws RepositoryException;
    }
}
