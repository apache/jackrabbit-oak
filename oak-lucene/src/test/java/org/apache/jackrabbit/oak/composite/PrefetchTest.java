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
package org.apache.jackrabbit.oak.composite;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.shutdown;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.prefetch.CacheWarming;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/**
 * Test if prefetch works, when using the composite node store.
 */
public class PrefetchTest extends CompositeNodeStoreQueryTestBase {

    @Parameters(name = "Root: {0}, Mounts: {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {NodeStoreKind.DOCUMENT_H2, NodeStoreKind.DOCUMENT_H2},
                {NodeStoreKind.DOCUMENT_H2, NodeStoreKind.SEGMENT},
                {NodeStoreKind.DOCUMENT_MEMORY, NodeStoreKind.DOCUMENT_MEMORY}
        });
    }

    private static String READ_ONLY_MOUNT_V1_NAME = "readOnlyV1";

    // JCR repository
    private CompositeRepo repoV1;
    private CompositeRepo repoV2;

    private ListAppender<ILoggingEvent> listAppender;
    private final String cacheWarmingLogger = CacheWarming.class.getName();


    @Rule
    public final ProvideSystemProperty updateSystemProperties
            = new ProvideSystemProperty(DocumentNodeStore.SYS_PROP_PREFETCH, "true");

    @Rule
    public final RestoreSystemProperties restoreSystemProperties
            = new RestoreSystemProperties();

    public PrefetchTest(NodeStoreKind root, NodeStoreKind mounts) {
        super(root, mounts);
    }

    @Rule
    public TemporarySystemProperty temporarySystemProperty = new TemporarySystemProperty();

    @Before
    public void loggingAppenderStart() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        listAppender = new ListAppender<>();
        listAppender.start();
        context.getLogger(cacheWarmingLogger).setLevel(Level.DEBUG);
        context.getLogger(cacheWarmingLogger).addAppender(listAppender);
    }

    @After
    public void loggingAppenderStop() {
        listAppender.stop();
    }

    @Override
    @Before
    public void initStore() throws Exception {
        globalStore = register(nodeStoreRoot.create(null));
        repoV1 = new CompositeRepo(READ_ONLY_MOUNT_V1_NAME);
        repoV1.initCompositeRepo();
    }

    @After
    public void tearDown() throws Exception {
        repoV1.cleanup();
        if (repoV2 != null) {
            repoV2.cleanup();
        }
    }

    @Test
    public void prefetch() throws Exception {
        // we run a query with prefetch, 
        // and see if the cacheWarmingLogger did log a message about it
        QueryResult result = repoV1.executeQuery("/jcr:root//*[@foo = 'bar'] option(prefetches 10)", "xpath");
        getResult(result, "jcr:path");
        assertTrue(isMessagePresent(listAppender, "Prefetch"));
    }

    private boolean isMessagePresent(ListAppender<ILoggingEvent> listAppender, String pattern) {
        for (ILoggingEvent loggingEvent : listAppender.list) {
            if (loggingEvent.getMessage().contains(pattern)) {
                return true;
            }
        }
        return false;
    }    

    private static String getResult(QueryResult result, String propertyName) throws RepositoryException {
        StringBuilder buff = new StringBuilder();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            buff.append(it.nextRow().getValue(propertyName).getString());
        }
        return buff.toString();
    }

    private class CompositeRepo {
        private Repository compositeRepository;
        private JackrabbitSession compositeSession;
        private QueryManager compositeQueryManager;

        private NodeStore readOnlyStore;
        private Repository readOnlyRepository;
        private CompositeNodeStore store;
        private MountInfoProvider mip;
        private JackrabbitSession readOnlySession;
        private Node readOnlyRoot;

        private boolean cleanedUp;

        public QueryResult executeQuery(String statement, String language) throws RepositoryException {
            return compositeQueryManager.createQuery(statement, language).execute();
        }

        CompositeRepo(String readOnlyMountName) throws Exception {
            this.readOnlyStore = register(mounts.create(readOnlyMountName));

            this.mip = Mounts.newBuilder().readOnlyMount(readOnlyMountName, "/libs").build();

            initReadOnlySeedRepo();
            List<MountedNodeStore> nonDefaultStores = new ArrayList<>();
            nonDefaultStores.add(new MountedNodeStore(this.mip.getMountByName(readOnlyMountName), readOnlyStore));
            this.store = new CompositeNodeStore(this.mip, globalStore, nonDefaultStores);

        }

        private void initCompositeRepo() throws Exception {
            compositeRepository = createJCRRepository(this.store, this.mip);
            compositeSession = (JackrabbitSession) compositeRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
            compositeQueryManager = compositeSession.getWorkspace().getQueryManager();
        }

        private void initReadOnlySeedRepo() throws Exception {
            readOnlyRepository = createJCRRepository(readOnlyStore, this.mip);
            readOnlySession = (JackrabbitSession) readOnlyRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
            readOnlyRoot = readOnlySession.getRootNode();
            Node libs = readOnlyRoot.addNode("libs", NT_UNSTRUCTURED);
            libs.setPrimaryType(NT_UNSTRUCTURED);
        }

        private void cleanup() {
            if (!cleanedUp) {
                compositeSession.logout();
                shutdown(compositeRepository);
                readOnlySession.logout();
                shutdown(readOnlyRepository);
            }
            cleanedUp = true;
        }

    }
}
