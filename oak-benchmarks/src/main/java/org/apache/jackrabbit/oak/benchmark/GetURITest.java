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

import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;

public class GetURITest extends AbstractTest {

    private static final int FILE_COUNT = Integer.getInteger("file.count", 100);

    private static final int FILE_SIZE = Integer.getInteger("file.size", 20);

    private static final boolean DEBUG = Boolean.getBoolean("debug");

    private Session session;

    private Node root;

    private List<Binary> binariesAdded = new ArrayList<>();

    private Random random = new Random(29);

    /**
     * Iteration counter used to avoid the split document edge case in DocumentMK.
     */
    private int iteration = 0;

    @Override @SuppressWarnings("deprecation")
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
        root = session.getRootNode().addNode(
            "GetURI" + TEST_ID + iteration++, "nt:folder");
        session.save();
        for (int i = 0; i < FILE_COUNT; i++) {
            Node file = root.addNode("file" + i, "nt:file");
            Node content = file.addNode("jcr:content", "nt:resource");
            content.setProperty("jcr:mimeType", "application/octet-stream");
            content.setProperty("jcr:lastModified", Calendar.getInstance());
            content.setProperty(
                "jcr:data", new TestInputStream(FILE_SIZE * 1024));
            Binary binary = root.getNode("file" + i).getNode("jcr:content").getProperty("jcr:data").getBinary();
            binariesAdded.add(binary);
        }
        session.save();

    }

    @Override
    public void runTest() throws Exception {
        int index = random.nextInt(FILE_COUNT);
        Binary binary = binariesAdded.get(index);
        URI uri = ((BinaryDownload) binary).getURI(BinaryDownloadOptions.DEFAULT);
        if (DEBUG) {
            System.out.println("URI retrieved=" + uri);
        }
    }

    @Override
    public void afterSuite() throws RepositoryException {
        root.remove();
        session.save();
    }

}
