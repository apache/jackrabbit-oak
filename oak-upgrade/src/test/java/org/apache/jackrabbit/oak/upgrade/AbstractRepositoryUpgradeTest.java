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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractRepositoryUpgradeTest {

    public static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    protected static NodeStore targetNodeStore;

    private static File testDirectory;

    private JackrabbitRepository targetRepository;

    @BeforeClass
    public static void init() throws InterruptedException {
        // ensure that we create a new repository for the next test
        targetNodeStore = null;
        testDirectory = createTestDirectory();
    }

    protected static File createTestDirectory() throws InterruptedException {
        final File dir = new File("target", "upgrade-" + Clock.SIMPLE.getTimeIncreasing());
        FileUtils.deleteQuietly(dir);
        return dir;
    }

    protected NodeStore createTargetNodeStore() {
        try {
            return SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public synchronized void upgradeRepository() throws Exception {
        if (targetNodeStore == null) {
            File directory = getTestDirectory();
            File source = new File(directory, "source");
            source.mkdirs();
            RepositoryImpl repository = createSourceRepository(source);
            Session session = repository.login(CREDENTIALS);
            try {
                createSourceContent(session);
            } finally {
                session.save();
                session.logout();
                repository.shutdown();
            }
            final NodeStore target = getTargetNodeStore();
            doUpgradeRepository(source, target);
            targetNodeStore = target;
        }
    }

    @After
    public synchronized void shutdownTargetRepository() {
        if (targetRepository != null) {
            targetRepository.shutdown();
            targetRepository = null;
        }
    }

    protected synchronized NodeStore getTargetNodeStore() {
        if (targetNodeStore == null) {
            targetNodeStore = createTargetNodeStore();
        }
        return targetNodeStore;
    }

    protected static File getTestDirectory() {
        return testDirectory;
    }

    protected RepositoryImpl createSourceRepository(File repositoryHome) throws IOException, RepositoryException {
        InputStream repoConfig = getRepositoryConfig();
        RepositoryConfig config;
        if (repoConfig == null) {
            config = RepositoryConfig.install(repositoryHome);
        } else {
            OutputStream out = FileUtils.openOutputStream(new File(repositoryHome, "repository.xml"));
            IOUtils.copy(repoConfig, out);
            out.close();
            repoConfig.close();
            config = RepositoryConfig.create(repositoryHome);
        }
        return RepositoryImpl.create(config);
    }


    protected void doUpgradeRepository(File source, NodeStore target)throws RepositoryException, IOException{
        RepositoryUpgrade.copy(source, target);
    }

    public InputStream getRepositoryConfig(){
        return null;
    }

    public Repository getTargetRepository() {
        if (targetRepository == null) {
            targetRepository = (JackrabbitRepository) new Jcr(new Oak(
                    targetNodeStore)).createRepository();
        }
        return targetRepository;
    }

    public JackrabbitSession createAdminSession()throws RepositoryException{
        return(JackrabbitSession)getTargetRepository().login(CREDENTIALS);
    }

    protected abstract void createSourceContent(Session session) throws Exception;

    protected void assertExisting(final String... paths) throws RepositoryException {
        final Session session = createAdminSession();
        try {
            assertExisting(session, paths);
        } finally {
            session.logout();
        }
    }

    protected void assertExisting(final Session session, final String... paths) throws RepositoryException {
        for (final String path : paths) {
            assertTrue("node " + path + " should exist", session.nodeExists(path));
        }
    }

    protected void assertMissing(final String... paths) throws RepositoryException {
        final Session session = createAdminSession();
        try {
            assertMissing(session, paths);
        } finally {
            session.logout();
        }
    }

    protected void assertMissing(final Session session, final String... paths) throws RepositoryException {
        for (final String path : paths) {
            assertFalse("node " + path + " should not exist", session.nodeExists(path));
        }
    }
}
