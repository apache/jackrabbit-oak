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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.commons.JcrUtils;

import com.google.common.io.ByteStreams;

abstract class ReadManyTest extends AbstractTest {

    private static final Random RANDOM = new Random(1234567890L);

    public interface Content {
        void create(Node parent, String name) throws RepositoryException;
        void read(Node node) throws RepositoryException;
    }

    public static final Content EMPTY = new Content() {
        @Override
        public void create(Node parent, String name) throws RepositoryException {
            parent.addNode(name);
        }
        @Override
        public void read(Node node) {
        }
    };

    public static final Content FILES = new Content() {
        @Override
        public void create(Node parent, String name) throws RepositoryException {
            byte[] randomBytes = new byte[10 * 1024];
            RANDOM.nextBytes(randomBytes);
            InputStream stream = new ByteArrayInputStream(randomBytes);
            JcrUtils.putFile(parent, name, "application/octet-stream", stream);
        }
        @Override
        public void read(Node node) throws RepositoryException {
            Binary binary = node.getProperty("jcr:content/jcr:data").getBinary();
            try {
                InputStream stream = binary.getStream();
                try {
                    ByteStreams.copy(stream, ByteStreams.nullOutputStream());
                } finally {
                    stream.close();
                }
            } catch (IOException e) {
                throw new RepositoryException("Unexpected IOException", e);
            } finally {
                binary.dispose();
            }
        }
    };

    public static final Content NODES = new Content() {
        @Override
        public void create(Node parent, String name) throws RepositoryException {
            Node node = parent.addNode(name);
            for (int i = 0; i < 10; i++) {
                char[] randomText = new char[1000];
                for (int j = 0; j < randomText.length; j++) {
                    if ((j % 10) != 0) {
                        randomText[j] = (char) ('a' + RANDOM.nextInt('z' - 'a'));
                    } else {
                        randomText[j] = ' ';
                    }
                }

                Node child = node.addNode("node" + i);
                child.setProperty("title", "child" + i);
                child.setProperty("content", new String(randomText));
            }
        }
        @Override
        public void read(Node node) throws RepositoryException {
            for (Node child : JcrUtils.getChildNodes(node)) {
                child.getProperty("title").getString();
                child.getProperty("content").getString();
            }
        }
    };

    public static ReadManyTest linear(String name, int scale, Content content) {
        return new ReadManyTest(name, scale, content) {
            @Override
            protected void runTest() throws Exception {
                Node top = root.getNode("node" + RANDOM.nextInt(scale));
                Node middle = top.getNode("node" + RANDOM.nextInt(1000));
                for (Node bottom : JcrUtils.getChildNodes(middle)) {
                    content.read(bottom);
                }
            }
        };
    }

    public static ReadManyTest uniform(String name, int scale, Content content) {
        return new ReadManyTest(name, scale, content) {
            @Override
            protected void runTest() throws Exception {
                for (int i = 0; i < 1000; i++) {
                    content.read(root
                            .getNode("node" + RANDOM.nextInt(scale))
                            .getNode("node" + RANDOM.nextInt(1000))
                            .getNode("node" + RANDOM.nextInt(1000)));
                }
            }
        };
    }

    private final String name;

    protected final int scale;

    protected final Content content;

    private Session session;

    protected Node root;

    protected ReadManyTest(String name, int scale, Content content) {
        this.name = name;
        this.scale = scale;
        this.content = content;
    }

    @Override
    protected void beforeSuite() throws Exception {
        session = getRepository().login(getCredentials());
        root = session.getRootNode().addNode("c" + TEST_ID);
        for (int i = 0; i < scale; i++) {
            Node top = root.addNode("node" + i);
            for (int j = 0; j < 1000; j++) {
                Node middle = top.addNode("node" + j);
                for (int k = 0; k < 1000; k++) {
                    content.create(middle, "node" + k);
                }
                session.save(); // save once every 1k leaf entries
            }
        }
    }

    @Override
    protected void afterSuite() throws Exception {
        for (int i = 0; i < scale; i++) {
            Node top = root.getNode("node" + i);
            for (int j = 0; j < 1000; j++) {
                top.getNode("node" + j).remove();
                // save once every 1k leaf entries (OAK-1056)
                session.save();
            }
        }
        root.remove();
        session.save();
        session.logout();
    }

    @Override
    public String toString() {
        return name;
    }

}
