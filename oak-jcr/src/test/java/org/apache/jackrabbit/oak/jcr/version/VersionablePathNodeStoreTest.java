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
package org.apache.jackrabbit.oak.jcr.version;

import static org.junit.Assert.assertTrue;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Test versionable paths with multiple node stores.
 * See OAK-3169 for details.
 */
public class VersionablePathNodeStoreTest extends AbstractRepositoryTest {

    private Session session;

    public VersionablePathNodeStoreTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        session = getAdminSession();
    }

    @Test
    public void testVersionablePaths() throws Exception {
        for (char i = 'a'; i <= 'z'; i++) {
            versionablePaths("" + i);
        }
    }

    private void versionablePaths(String nodeName) throws Exception {
        Node root = session.getRootNode();
        Node n = root.addNode(nodeName, "nt:unstructured");
        n.addMixin("mix:versionable");
        session.save();
        String p = n.getProperty("jcr:versionHistory").getString();
        Node n2 = session.getNodeByIdentifier(p);
        assertTrue("nodeName " + nodeName,
                n2.isNodeType("rep:VersionablePaths"));
        n.remove();
        session.save();
    }

}
