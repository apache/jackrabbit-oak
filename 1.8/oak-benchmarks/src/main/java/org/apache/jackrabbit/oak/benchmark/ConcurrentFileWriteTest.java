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

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.Random;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.commons.JcrUtils;

/**
 * Test case that writes blobs concurrently and concurrently reads
 * the blobs back when available.
 */
public class ConcurrentFileWriteTest extends AbstractTest {

    private static final int FILE_SIZE = Integer.getInteger("fileSize", 1900);

    private static final int WRITERS = Integer.getInteger("fileWriters", 50);

    private static final int READERS = Integer.getInteger("fileReaders", 50);

    protected static final String ROOT_NODE_NAME =
            "concurrentFileWriteTest" + TEST_ID;

    private final Random random = new Random();

    private final List<String> paths = newArrayList();

    private Writer writer;

    @Override
    public void beforeSuite() throws RepositoryException {
        Session session = loginWriter();
        session.getRootNode().addNode(ROOT_NODE_NAME);
        session.save();

        this.writer = new Writer(0);
        writer.run();

        for (int i = 1; i < WRITERS; i++) {
            addBackgroundJob(new Writer(i));
        }
        for (int i = 0; i < READERS; i++) {
            addBackgroundJob(new Reader());
        }
    }

    @Override
    public void runTest() throws Exception {
        writer.run();
    }

    private synchronized String getRandomPath() {
        return paths.get(random.nextInt(paths.size()));
    }

    private synchronized void addPath(String path) {
        paths.add(path);
    }

    private class Reader implements Runnable {

        private final Session session = loginWriter();

        @Override
        public void run() {
            try {
                String path = getRandomPath();
                session.refresh(false);
                JcrUtils.readFile(
                        session.getNode(path), new NullOutputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private class Writer implements Runnable {

        private final Node parent;

        private long counter = 0;

        Writer(int id) throws RepositoryException {
            this.parent = loginWriter()
                    .getRootNode()
                    .getNode(ROOT_NODE_NAME)
                    .addNode("writer-" + id);
            parent.getSession().save();
        }

        @Override
        public void run() {
            try {
                parent.getSession().refresh(false);
                Node file = JcrUtils.putFile(
                        parent, "file-" + counter++,
                        "application/octet-stream",
                        new TestInputStream(FILE_SIZE * 1024));
                parent.getSession().save();
                addPath(file.getPath());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
