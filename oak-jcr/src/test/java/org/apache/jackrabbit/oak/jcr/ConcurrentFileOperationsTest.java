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
package org.apache.jackrabbit.oak.jcr;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * File related write operations on the repository.
 */
public class ConcurrentFileOperationsTest extends AbstractRepositoryTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentFileOperationsTest.class);
    private static final int NUM_WRITERS = 10;
    private static final byte[] DATA = new byte[8];

    static {
        new Random(0).nextBytes(DATA);
    }

    private Session session;
    private Node testRootNode;

    public ConcurrentFileOperationsTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        session = getAdminSession();
        testRootNode = JcrUtils.getOrAddNode(session.getRootNode(),
                "test-node", "nt:unstructured");
        session.save();
    }

    /**
     * Multiple threads create and rename files.
     */
    @Test
    public void concurrent() throws Exception {
        // OAK-893
        assumeTrue(fixture != NodeStoreFixture.MONGO_MK);
        List<Session> sessions = new ArrayList<Session>();
        for (int i = 0; i < NUM_WRITERS; i++) {
            sessions.add(createAdminSession());
            testRootNode.addNode("session-" + i, "nt:unstructured");
        }
        addFile(testRootNode, "dummy");
        session.save();
        final Map<String, Exception> exceptions = Collections.synchronizedMap(
                new HashMap<String, Exception>());
        List<Thread> writers = new ArrayList<Thread>();
        for (int i = 0; i < sessions.size(); i++) {
            final Session s = sessions.get(i);
            final String path = testRootNode.getPath() + "/session-" + i;
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        s.refresh(false);
                        Node n = s.getNode(path);
                        for (int i = 0; i < 10; i++) {
                            String tmpFile = "file-" + i + ".tmp";
                            // create
                            addFile(n, tmpFile);
                            s.save();
                            String srcPath = n.getPath() + "/" + tmpFile;
                            String destPath = n.getPath() + "/file-" + i + ".bin";
                            // rename
                            s.move(srcPath, destPath);
                            s.save();
                        }

                    } catch (RepositoryException e) {
                        exceptions.put(path, e);
                    }
                }
            }));
        }
        for (Thread t : writers) {
            t.start();
        }
        for (Thread t : writers) {
            t.join();
        }
        for (Session s : sessions) {
            s.logout();
        }
        for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
            log.info("Worker (" + entry.getKey() + ") failed with exception: " + entry.getValue().toString());
            throw entry.getValue();
        }
    }

    @Test
    public void interleavingOperations1() throws Exception {
        Node folder1 = testRootNode.addNode("folder1", "nt:unstructured");
        Node folder2 = testRootNode.addNode("folder2", "nt:unstructured");
        Node file1 = addFile(session.getNode(folder1.getPath()), "file1.tmp");
        Node file2 = addFile(session.getNode(folder2.getPath()), "file2.tmp");
        session.save();

        Session s1 = createAdminSession();
        Session s2 = createAdminSession();
        try {
            rename(s1.getNode(file1.getPath()), "file1.bin");
            rename(s2.getNode(file2.getPath()), "file2.bin");
            s1.save();
            s2.save();
        } finally {
            s1.logout();
            s2.logout();
        }
    }

    @Test
    public void interleavingOperations2() throws Exception {
        Node folder1 = testRootNode.addNode("folder1", "nt:unstructured");
        Node folder2 = testRootNode.addNode("folder2", "nt:unstructured");
        addFile(testRootNode, "dummy");
        session.save();

        Session s1 = createAdminSession();
        Session s2 = createAdminSession();
        try {
            Node file1 = addFile(s1.getNode(folder1.getPath()), "file1.tmp");
            Node file2 = addFile(s2.getNode(folder2.getPath()), "file2.tmp");
            s1.save();
            s2.save();
            rename(file1, "file1.bin");
            rename(file2, "file2.bin");
            s1.save();
            s2.save();
        } finally {
            s1.logout();
            s2.logout();
        }
        session.refresh(false);
        assertTrue(session.nodeExists(folder1.getPath() + "/file1.bin"));
        assertFalse(session.nodeExists(folder1.getPath() + "/file1.tmp"));
        assertTrue(session.nodeExists(folder2.getPath() + "/file2.bin"));
        assertFalse(session.nodeExists(folder2.getPath() + "/file2.tmp"));
    }

    private static Node addFile(Node parent, String name)
            throws RepositoryException {
        return JcrUtils.putFile(parent, name,
                "application/octet-stream", new ByteArrayInputStream(DATA));
    }

    private static void rename(Node node, String name)
            throws RepositoryException {
        String destPath = PathUtils.getParentPath(node.getPath()) + "/" + name;
        node.getSession().move(node.getPath(), destPath);
    }
}
