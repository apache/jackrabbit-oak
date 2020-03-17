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

import java.io.InputStream;
import java.util.Calendar;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.io.ByteStreams;

public class SmallFileReadTest extends AbstractTest {

    private static final int FILE_COUNT = 1000;

    private static final int FILE_SIZE = Integer.getInteger("file.size", 10);

    private Session session;

    private Node root;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = getRepository().login(getCredentials());

        root = session.getRootNode().addNode(
                getClass().getSimpleName() + TEST_ID, "nt:folder");
        for (int i = 0; i < FILE_COUNT; i++) {
            Node file = root.addNode("file" + i, "nt:file");
            Node content = file.addNode("jcr:content", "nt:resource");
            content.setProperty("jcr:mimeType", "application/octet-stream");
            content.setProperty("jcr:lastModified", Calendar.getInstance());
            content.setProperty(
                    "jcr:data", new TestInputStream(FILE_SIZE * 1024));
        }
        session.save();
    }

    @Override
    public void runTest() throws Exception {
        for (int i = 0; i < FILE_COUNT; i++) {
            Node file = root.getNode("file" + i);
            Node content = file.getNode("jcr:content");
            InputStream stream = content.getProperty("jcr:data").getStream();
            try {
                ByteStreams.copy(stream, ByteStreams.nullOutputStream());
            } finally {
                stream.close();
            }
        }
    }

    @Override
    public void afterSuite() throws RepositoryException {
        root.remove();
        session.save();
        session.logout();
    }

}
