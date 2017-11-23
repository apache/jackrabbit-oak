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
import java.util.Calendar;
import java.util.Random;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Simple test case which adds a basic hierarchy of 3 nodes (nt:folder, nt:file
 * and nt:resource) and 3 properties (jcr:mimeType, jcr:lastModified and
 * jcr:data). The binary data accounts for 5MB and is randomly generated for
 * each run.
 */
public class BasicWriteTest extends AbstractTest {
    private static final int MB = 1024 * 1024;
    private Session session;
    private Node root;

    /**
     * Iteration counter used to avoid the slit document edge case in
     * DocumentMK.
     */
    private int iteration = 0;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
    }

    @Override
    public void beforeTest() throws RepositoryException {
        root = session.getRootNode().addNode(TEST_ID + iteration++, "nt:folder");
        session.save();
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void runTest() throws Exception {
        final int blobSize = 5 * MB;
        
        Node file = root.addNode("server", "nt:file");

        byte[] data = new byte[blobSize];
        new Random().nextBytes(data);

        Node content = file.addNode("jcr:content", "nt:resource");
        content.setProperty("jcr:mimeType", "application/octet-stream");
        content.setProperty("jcr:lastModified", Calendar.getInstance());
        content.setProperty("jcr:data", new ByteArrayInputStream(data));

        session.save();
    }

    @Override
    public void afterTest() throws RepositoryException {
        root.remove();
        session.save();
    }
}
