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
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assume.assumeTrue;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates behavior similar to an import of many nodes (batched).
 *
 * These tests are disabled by default due to their long running time. On the command line
 * specify {@code -DPackageImportIT=true} to enable them.
 */

public class PackageImportIT extends AbstractRepositoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(PackageImportIT.class);
    private static final boolean ENABLED = Boolean.getBoolean(PackageImportIT.class.getSimpleName());

    public PackageImportIT(NodeStoreFixture fixture) {
        super(fixture);
        assumeTrue(ENABLED);
    }

    private static final String TEST_NODE = "import-test";
    private static final String TEST_PATH = '/' + TEST_NODE;

    private static int DURATION = Integer.getInteger("import.duration", 10000);
    private static int BATCHSIZE = Integer.getInteger("import.batchsize", 1024);
    private static boolean NOTREFERENCEABLE = Boolean.getBoolean("import.notreferenceable");

    private Node testNode;

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        testNode = root.addNode(TEST_NODE);
        session.save();
        LOG.info("Starting test for{}. Duration: {}, batch size: {}, referenceable: {}, update.limit: {}.", super.fixture, DURATION,
                BATCHSIZE, !NOTREFERENCEABLE, Integer.getInteger("update.limit"));
    }

    @After
    public void tearDown() throws RepositoryException {
        Session s = testNode.getSession();
        s.removeItem(TEST_PATH);
        s.save();
    }

    @Test
    public void testImport() throws RepositoryException {
        long start = System.currentTimeMillis();
        long until = start + DURATION;
        long total = 0;

        while (System.currentTimeMillis() < until) {
            start = System.currentTimeMillis();
            String cname = String.format("fld-%x", start);

            Node container = testNode.addNode(cname, JcrConstants.NT_FOLDER);
            for (int i = 0; i < BATCHSIZE; i++) {
                Node f = container.addNode("f" + i, JcrConstants.NT_FILE);
                if (!NOTREFERENCEABLE) {
                    f.addMixin(JcrConstants.MIX_REFERENCEABLE);
                }
                Node c = f.addNode(JcrConstants.JCR_CONTENT, JcrConstants.NT_UNSTRUCTURED);
                c.setProperty("foo", "bar");
            }
            testNode.getSession().save();
            long elapsed = System.currentTimeMillis() - start;
            LOG.info("Saved {} nodes ({}) in {}ms ({})", BATCHSIZE, NOTREFERENCEABLE ? "not referenceable" : "referenceable",
                    elapsed, super.fixture);
            total += BATCHSIZE;
        }
        LOG.info("A total of {} nodes were saved ({}).", total, super.fixture);
    }
}
