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

import static java.lang.Thread.UncaughtExceptionHandler;
import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Concurrently add nodes with multiple sessions on multiple cluster nodes.
 */
public class ConcurrentAddNodesClusterIT {

    private static final int NUM_CLUSTER_NODES = 5;
    private static final int NODE_COUNT = 100;
    private static final int LOOP_COUNT = 10;
    private static final int WORKER_COUNT = 20;
    private static final String PROP_NAME = "testcount";

    private List<Repository> repos = new ArrayList<Repository>();
    private List<DocumentMK> mks = new ArrayList<DocumentMK>();
    private List<Thread> workers = new ArrayList<Thread>();

    @BeforeClass
    public static void mongoDBAvailable() {
        Assume.assumeTrue(OakMongoNSRepositoryStub.isMongoDBAvailable());
    }

    @Before
    public void before() throws Exception {
        dropDB();
        initRepository();
    }

    @After
    public void after() throws Exception {
        workers.clear();
        for (Repository repo : repos) {
            dispose(repo);
        }
        repos.clear();
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        mks.clear();
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

    @Ignore("OAK-1807")
    @Test
    public void addNodesConcurrent2() throws Exception {
        final Thread mainThread = Thread.currentThread();
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            DocumentMK mk = new DocumentMK.Builder()
                    .setMongoDB(createConnection().getDB())
                    .setClusterId(i + 1).open();
            mks.add(mk);
        }
        final Map<String, Exception> exceptions = Collections.synchronizedMap(
                new HashMap<String, Exception>());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean stop = new AtomicBoolean();
        final UncaughtExceptionHandler ueh = new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                RuntimeException r = new RuntimeException("Exception in thread "+t.getName(), e);
                r.printStackTrace();
            }
        };
        for (int i = 0; i < mks.size(); i++) {
            DocumentMK mk = mks.get(i);
            final Repository repo = new Jcr(mk.getNodeStore()).createRepository();
            repos.add(repo);
            for (int w = 0; w <= WORKER_COUNT; w++) {
                final String name = "Worker-" + (i + 1) + "-" + (w + 1);
                final Runnable r = new Runnable() {
                    final Session session = createAdminSession(repo);
                    int count = 0;
                    @Override
                    public void run() {
                        try {
                            Uninterruptibles.awaitUninterruptibly(latch);
                            session.refresh(false);
                            Node node = session.getRootNode().addNode(name+count++, "oak:Unstructured");
                            for (int j = 0; j < NODE_COUNT && !stop.get() ; j++) {
                                node.addNode("node" + j);
                                session.save();
                            }
                        } catch (RepositoryException e) {
                            RuntimeException r = new RuntimeException("Exception in thread "+name, e);
                            r.printStackTrace();
                            exceptions.put(Thread.currentThread().getName(), r);
                            stop.set(true);
                            mainThread.interrupt();
                        } finally {
                            session.logout();
                        }
                    }
                };

                //Last runnable would be a long running one
                Runnable runnable = r;
                if(w == WORKER_COUNT){
                    runnable = new Runnable() {
                        @Override
                        public void run() {
                            while(!stop.get()){
                                r.run();
                            }
                        }
                    };
                }

                Thread t = new Thread(runnable);
                t.setName(name);
                t.setUncaughtExceptionHandler(ueh);
                workers.add(t);
            }
        }
        for (Thread t : workers) {
            t.start();
        }
        latch.countDown();

        TimeUnit.MINUTES.sleep(10);
        stop.set(true);

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
        ensureIndex(s2.getRootNode(), PROP_NAME);

        Map<String, Exception> exceptions = Collections.synchronizedMap(
                new HashMap<String, Exception>());
        createNodes(s1, "testroot-1", 1, 1, exceptions);
        createNodes(s2, "testroot-2", 1, 1, exceptions);

        for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
            throw entry.getValue();
        }

        s1.logout();
        s2.logout();
    }

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
        runBackgroundOps(mk1);
        runBackgroundOps(mk2);
        runBackgroundOps(mk3);

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

        s1.logout();
        s2.logout();
        s3.logout();
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

        s1.logout();
        s2.logout();
    }

    private static MongoConnection createConnection() throws Exception {
        return OakMongoNSRepositoryStub.createConnection(
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
        Repository repository = new Jcr(mk.getNodeStore()).createRepository();
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        session.logout();
        dispose(repository);
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
                Session session = createAdminSession(repo);
                try {
                    ensureIndex(session.getRootNode(), PROP_NAME);
                    String nodeName = "testroot-" + Thread.currentThread().getName();
                    createNodes(session, nodeName, LOOP_COUNT, NODE_COUNT, exceptions);
                } finally {
                    session.logout();
                }
            } catch (Exception e) {
                exceptions.put(Thread.currentThread().getName(), e);
            }
        }

    }

    private Session createAdminSession(Repository repository) throws RepositoryException {
        return repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
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
