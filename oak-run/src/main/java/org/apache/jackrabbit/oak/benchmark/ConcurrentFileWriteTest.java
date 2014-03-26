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
package org.apache.jackrabbit.oak.benchmark;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Test case that writes blobs concurrently and concurrently reads the blobs back when available.
 */
public class ConcurrentFileWriteTest extends AbstractTest {

    private static final String NT_FOLDER = "nt:folder";
    private static final String NT_FILE = "nt:file";
    private static final String NT_RESOURCE = "nt:resource";
    private static final String JCR_DATA = "jcr:data";
    private static final String JCR_CONTENT = "jcr:content";
    private static final String JCR_MIME_TYPE = "jcr:mimeType";
    private static final String JCR_LAST_MOD = "jcr:lastModified";

    private static final int FILE_SIZE = Integer.getInteger("fileSize", 1900);

    private static final int FILE_COUNT = Integer.getInteger("fileCount", 10);

    private static final int WRITERS = Integer.getInteger("fileWriters", 50);

    private static final int READERS = Integer.getInteger("fileReaders", 50);

    protected static final String ROOT_NODE_NAME = "concurrentFileWriteTest" + TEST_ID;

    private Session session;

    @Override
    public void beforeTest() {
        session = loginWriter();
        try {
            session.getRootNode().addNode(ROOT_NODE_NAME, NT_FOLDER);
            session.save();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void runTest() throws Exception {
        // randomize the root folder for this run of the test
        String runId = String.valueOf((new Random()).nextInt());
        session.getRootNode().getNode(ROOT_NODE_NAME).addNode(runId, NT_FOLDER);
        session.save();

        CountDownLatch writersStopLatch = new CountDownLatch(WRITERS);
        AtomicBoolean stopReadersFlag = new AtomicBoolean(false);

        for (int i = 0; i < WRITERS; i++) {
            Thread t = new Thread(new Writer(writersStopLatch, i, runId),
                            "ConcurrentFileWriteTest-Writer-" + i);
            t.start();
        }

        for (int i = 0; i < READERS; i++) {
            Thread t = new Thread(new Reader(i, stopReadersFlag, runId),
                            "ConcurrentFileWriteTest-Reader-" + i);
            t.start();
        }
        writersStopLatch.await();
        stopReadersFlag.set(true);
    }

    @Override
    public void afterTest() throws RepositoryException {
        session.refresh(true);
        if (session.getRootNode().hasNode(ROOT_NODE_NAME)) {
            session.getRootNode().getNode(ROOT_NODE_NAME).remove();
        }
        session.save();
    }

    class Reader implements Runnable {
        private int id;
        private Session session;
        private AtomicBoolean stopFlag;
        private String runId;

        public Reader(int id, AtomicBoolean stopFlag, String runId) {
            this.id = id;
            session = loginWriter();
            this.stopFlag = stopFlag;
            this.runId = runId;
        }

        @Override
        public void run() {
            readFile();
        }

        private void readFile() {
            for (int i = 0; i < FILE_COUNT && !stopFlag.get(); i++) {
                try {
                    Node fileRoot = session.getRootNode().getNode(ROOT_NODE_NAME);
                    String fileid = "file" + id + "-" + i;

                    while (!stopFlag.get() && !fileRoot.hasNode(fileid)) {
                        Thread.sleep(50);
                    }

                    if (!stopFlag.get()) {
                        session.getRootNode().getNode(ROOT_NODE_NAME).getNode(runId).getNode(fileid)
                                .getNode(JCR_CONTENT).getProperty(JCR_DATA).getBinary();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class Writer implements Runnable {
        private Session session;
        private CountDownLatch latch;
        private int id;
        private String runId;

        public Writer(CountDownLatch stopLatch, int id, String runId) throws Exception {
            latch = stopLatch;
            session = loginWriter();
            this.id = id;
            this.runId = runId;
        }

        @Override
        public void run() {
            createFile();
            latch.countDown();
        }

        @SuppressWarnings("deprecation")
        private void createFile() {
            for (int i = 0; i < FILE_COUNT; i++) {
                try {
                    session.refresh(false);

                    Node file =
                            session.getRootNode().getNode(ROOT_NODE_NAME).getNode(runId)
                                    .addNode("file" + id + "-" + i, NT_FILE);
                    Node content = file.addNode(JCR_CONTENT, NT_RESOURCE);
                    content.setProperty(JCR_MIME_TYPE, "application/octet-stream");
                    content.setProperty(JCR_LAST_MOD, Calendar.getInstance());
                    content.setProperty(JCR_DATA, new TestInputStream(FILE_SIZE * 1024));
                    session.save();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
