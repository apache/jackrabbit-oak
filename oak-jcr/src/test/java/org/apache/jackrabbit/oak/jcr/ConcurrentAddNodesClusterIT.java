/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Concurrently add nodes with multiple sessions on multiple cluster nodes.
 */
public class ConcurrentAddNodesClusterIT {

    private static final int NUM_CLUSTER_NODES = 3;
    private static final int NODE_COUNT = 100;
    private static final int LOOP_COUNT = 10;
    private static final String PROP_NAME = "testcount";
    private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private List<Repository> repos = new ArrayList<Repository>();
    private List<DocumentMK> mks = new ArrayList<DocumentMK>();
    private List<Thread> workers = new ArrayList<Thread>();

    @BeforeClass
    public static void mongoDBAvailable() {
        Assume.assumeTrue(OakMongoMKRepositoryStub.isMongoDBAvailable());
    }

    @Before
    public void before() throws Exception {
        dropDB();
        initRepository();
    }

    @After
    public void after() throws Exception {
        for (Repository repo : repos) {
            dispose(repo);
        }
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        dropDB();
    }

    @Test
    public void addNodesConcurrent() throws Exception {
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            DocumentMK mk = new DocumentMK.Builder()
                    .setMongoDB(createConnection().getDB())
                    .setClusterId(i + 1).open();
            mks.add(mk);
        }
        Map<String, Exception> exceptions = Collections.synchronizedMap(
                new HashMap<String, Exception>());
        for (int i = 0; i < mks.size(); i++) {
            DocumentMK mk = mks.get(i);
            Repository repo = new Jcr(mk.getNodeStore()).createRepository();
            repos.add(repo);
            workers.add(new Thread(new Worker(repo, exceptions), "Worker-" + (i + 1)));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
            // System.out.println("exception in thread " + entry.getKey());
            throw entry.getValue();
        }
    }

    @Test
    public void addNodes() throws Exception {
        for (int i = 0; i < 2; i++) {
            DocumentMK mk = new DocumentMK.Builder()
                    .setMongoDB(createConnection().getDB())
                    .setAsyncDelay(0)
                    .setClusterId(i + 1).open();
            mks.add(mk);
        }
        final DocumentMK mk1 = mks.get(0);
        final DocumentMK mk2 = mks.get(1);
        Repository r1 = new Jcr(mk1.getNodeStore()).createRepository();
        repos.add(r1);
        Repository r2 = new Jcr(mk2.getNodeStore()).createRepository();
        repos.add(r2);

        Session s1 = r1.login(new SimpleCredentials("admin", "admin".toCharArray()));
        Session s2 = r2.login(new SimpleCredentials("admin", "admin".toCharArray()));

        ensureIndex(s1.getRootNode(), PROP_NAME);
        syncMKs(1);
        ensureIndex(s2.getRootNode(), PROP_NAME);

        Map<String, Exception> exceptions = Collections.synchronizedMap(
                new HashMap<String, Exception>());
        createNodes(s1, "testroot-1", 1, 1, exceptions);
        syncMKs(1);
        createNodes(s2, "testroot-2", 1, 1, exceptions);

        for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
            throw entry.getValue();
        }
    }

    @Ignore("OAK-1579")
    @Test
    public void addNodes2() throws Exception {
        for (int i = 0; i < 3; i++) {
            DocumentMK mk = new DocumentMK.Builder()
                    .setMongoDB(createConnection().getDB())
                    .setAsyncDelay(0)
                    .setClusterId(i + 1).open();
            mks.add(mk);
        }
        final DocumentMK mk1 = mks.get(0);
        final DocumentMK mk2 = mks.get(1);
        final DocumentMK mk3 = mks.get(2);
        Repository r1 = new Jcr(mk1.getNodeStore()).createRepository();
        repos.add(r1);
        Repository r2 = new Jcr(mk2.getNodeStore()).createRepository();
        repos.add(r2);
        Repository r3 = new Jcr(mk3.getNodeStore()).createRepository();
        repos.add(r3);

        Session s1 = r1.login(new SimpleCredentials("admin", "admin".toCharArray()));
        Session s2 = r2.login(new SimpleCredentials("admin", "admin".toCharArray()));
        Session s3 = r3.login(new SimpleCredentials("admin", "admin".toCharArray()));

        ensureIndex(s1.getRootNode(), PROP_NAME);
        syncMKs(1);
        ensureIndex(s2.getRootNode(), PROP_NAME);
        ensureIndex(s3.getRootNode(), PROP_NAME);

        // begin test

        Node root2 = s2.getRootNode().addNode("testroot-Worker-2", "nt:unstructured");
        createNodes(root2, "testnode0");
        s2.save();

        createNodes(root2, "testnode1");

        runBackgroundOps(mk1);
        runBackgroundOps(mk3);
        runBackgroundOps(mk2); // publish 'testroot-Worker-2/testnode0'

        Node root3 = s3.getRootNode().addNode("testroot-Worker-3", "nt:unstructured");
        createNodes(root3, "testnode0");

        s2.save();
        createNodes(root2, "testnode2");

        runBackgroundOps(mk1); // sees 'testroot-Worker-2/testnode0'
        runBackgroundOps(mk3); // sees 'testroot-Worker-2/testnode0'
        runBackgroundOps(mk2); // publish 'testroot-Worker-2/testnode1'

        // subsequent read on mk3 will read already published docs from mk2
        s3.save();
        createNodes(root3, "testnode1");

        Node root1 = s1.getRootNode().addNode("testroot-Worker-1", "nt:unstructured");
        createNodes(root1, "testnode0");

        s2.save();
        createNodes(root2, "testnode3");

        runBackgroundOps(mk1);
        runBackgroundOps(mk3);
        runBackgroundOps(mk2);

        s1.save();
        createNodes(root1, "testnode1");

        s3.save();
        createNodes(root3, "testnode2");

        runBackgroundOps(mk1);

        s1.save();
    }

    @Test
    public void rebaseVisibility() throws Exception {
        for (int i = 0; i < 2; i++) {
            DocumentMK mk = new DocumentMK.Builder()
                    .setMongoDB(createConnection().getDB())
                    .setAsyncDelay(0)
                    .setClusterId(i + 1).open();
            mks.add(mk);
        }
        final DocumentMK mk1 = mks.get(0);
        final DocumentMK mk2 = mks.get(1);
        Repository r1 = new Jcr(mk1.getNodeStore()).createRepository();
        repos.add(r1);
        Repository r2 = new Jcr(mk2.getNodeStore()).createRepository();
        repos.add(r2);

        Session s1 = r1.login(new SimpleCredentials("admin", "admin".toCharArray()));
        Session s2 = r2.login(new SimpleCredentials("admin", "admin".toCharArray()));

        Node root1 = s1.getRootNode().addNode("session-1");
        s1.save();
        Node root2 = s2.getRootNode().addNode("session-2");
        s2.save();

        runBackgroundOps(mk1);
        runBackgroundOps(mk2);
        runBackgroundOps(mk1);

        createNodes(root1, "nodes");

        createNodes(root2, "nodes");
        s2.save();

        runBackgroundOps(mk2);
        runBackgroundOps(mk1);

        assertFalse(s1.getRootNode().hasNode("session-2/nodes"));

        s1.refresh(true);
        assertTrue(s1.getRootNode().hasNode("session-2/nodes"));
    }

    private void syncMKs(int delay) {
        EXECUTOR.schedule(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                for (DocumentMK mk : mks) {
                    runBackgroundOps(mk);
                }
                return null;
            }
        }, delay, TimeUnit.SECONDS);
    }

    private static MongoConnection createConnection() throws Exception {
        return OakMongoMKRepositoryStub.createConnection(
                ConcurrentAddNodesClusterIT.class.getSimpleName());
    }

    private static void dropDB() throws Exception {
        MongoConnection con = createConnection();
        try {
            con.getDB().dropDatabase();
        } finally {
            con.close();
        }
    }

    private static void initRepository() throws Exception {
        MongoConnection con = createConnection();
        DocumentMK mk = new DocumentMK.Builder()
                .setMongoDB(con.getDB())
                .setClusterId(1).open();
        Session session = new Jcr(mk.getNodeStore()).createRepository().login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        session.logout();
        mk.dispose(); // closes connection as well
    }


    private static void ensureIndex(Node root, String propertyName)
            throws RepositoryException {
        Node indexDef = root.getNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        if (indexDef.hasNode(propertyName)) {
            return;
        }
        Node index = indexDef.addNode(propertyName, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        index.setProperty(IndexConstants.TYPE_PROPERTY_NAME,
                PropertyIndexEditorProvider.TYPE);
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME,
                true);
        index.setProperty(IndexConstants.PROPERTY_NAMES,
                new String[] { propertyName }, PropertyType.NAME);
        try {
            root.getSession().save();
        } catch (RepositoryException e) {
            // created by other thread -> ignore
            root.getSession().refresh(false);
        }
    }

    private static void runBackgroundOps(DocumentMK mk) throws Exception {
        mk.getNodeStore().runBackgroundOperations();
    }

    private final class Worker implements Runnable {

        private final Repository repo;
        private final Map<String, Exception> exceptions;

        Worker(Repository repo, Map<String, Exception> exceptions) {
            this.repo = repo;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            try {
                Session session = repo.login(new SimpleCredentials(
                        "admin", "admin".toCharArray()));
                ensureIndex(session.getRootNode(), PROP_NAME);

                String nodeName = "testroot-" + Thread.currentThread().getName();
                createNodes(session, nodeName, LOOP_COUNT, NODE_COUNT, exceptions);
            } catch (Exception e) {
                exceptions.put(Thread.currentThread().getName(), e);
            }
        }
    }

    private void createNodes(Session session,
                             String nodeName,
                             int loopCount,
                             int nodeCount,
                             Map<String, Exception> exceptions)
            throws RepositoryException {
        Node root = session.getRootNode().addNode(nodeName, "nt:unstructured");
        for (int i = 0; i < loopCount; i++) {
            Node node = root.addNode("testnode" + i, "nt:unstructured");
            for (int j = 0; j < nodeCount; j++) {
                Node child = node.addNode("node" + j, "nt:unstructured");
                child.setProperty(PROP_NAME, j);
            }
            if (!exceptions.isEmpty()) {
                break;
            }
            session.save();
        }
    }

    private void createNodes(Node parent, String child)
            throws RepositoryException {
        Node node = parent.addNode(child, "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            Node c = node.addNode("node" + i, "nt:unstructured");
            c.setProperty(PROP_NAME, i);
        }
    }
}
